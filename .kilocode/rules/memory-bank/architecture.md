# System Architecture: Realize File Syncer

## Overview

Realize is designed as a decentralized file synchronization system enabling partial syncing between peers. Each peer runs a daemon that manages local storage ("Real" files) and a cache of remote files ("Unreal" files), presenting a unified filesystem view. Communication and data exchange occur via a custom RPC protocol over TLS.

## Core Concepts

*   **Arena:** A distinct set of files and their associated data shared among peers.
*   **Peer:** A member of a "Household" (a group of peers syncing arenas).
*   **Real Files:** Files fully stored locally.
*   **Unreal Files:** Files whose content is not fully stored locally but metadata is available, and content can be fetched or served from a cache.
*   **Blob:** A piece of data identified by its hash (Merkle tree based).
*   **Consensus:** The state where all peers agree on the set of paths and their assigned blobs within an arena.
*   **History:** A log of local modifications (create, delete, rename, edit, overwrite) kept by each peer.

## Layers and Components

The system is structured in layers, primarily within the `realize-lib` crate:

```mermaid
graph TD
    subgraph Interface Layer
        A[Realize Filesystem (FUSE/NFS)]
        B[Application & CLI Tools (realize-cmd, realized)]
    end

    subgraph Logic Layer
        C[The Fastness (Consolidated View & Core Logic)]
    end

    subgraph Data Layers
        D[Storage Layer]
        E[Network Layer]
    end

    subgraph Storage Details
        F[The Real (Local Files)]
        G[The Unreal Cache (redb)]
        H[History (redb/files)]
    end

    subgraph Network Details
        I[RPC Services (RealizeService, HistoryService)]
        J[Transport (TCP+TLS)]
    end

    A --> C
    B --> C
    C --> D
    C --> E
    D --> F
    D --> G
    D --> H
    E --> I
    E --> J
```

### Source Code Paths (primarily in `crate/realize-lib/src/`)

*   **`model/`**: Defines core data structures.
    *   [`model/arena.rs`](crate/realize-lib/src/model/arena.rs): `Arena` identifier.
    *   [`model/byterange.rs`](crate/realize-lib/src/model/byterange.rs): `ByteRange` and `ByteRanges` for partial file operations.
    *   [`model/data.rs`](crate/realize-lib/src/model/data.rs): `Hash`, `Signature`, `Delta` for data integrity and rsync.
    *   [`model/path.rs`](crate/realize-lib/src/model/path.rs): Custom `Path` type.
    *   [`model/peer.rs`](crate/realize-lib/src/model/peer.rs): `Peer` identifier.
*   **`network/`**: Handles peer-to-peer communication.
    *   [`network/config.rs`](crate/realize-lib/src/network/config.rs): `PeerConfig` for peer connection details.
    *   [`network/rpc/realize.rs`](crate/realize-lib/src/network/rpc/realize.rs): Defines the `RealizeService` trait (using `tarpc`).
    *   [`network/rpc/realize/server.rs`](crate/realize-lib/src/network/rpc/realize/server.rs): Server-side implementation of `RealizeService`.
    *   [`network/rpc/realize/metrics.rs`](crate/realize-lib/src/network/rpc/realize/metrics.rs): Prometheus metrics for RPC.
    *   [`network/security.rs`](crate/realize-lib/src/network/security.rs): TLS setup and raw public key peer verification.
    *   [`network/tcp.rs`](crate/realize-lib/src/network/tcp.rs): TCP transport for `tarpc`.
    *   `network/rate_limit.rs`: (Internal) Rate limiting for network traffic.
    *   `network/reconnect.rs`: (Internal) Logic for reconnecting to peers.
*   **`storage/`**: Manages local file storage and caching.
    *   [`storage/config.rs`](crate/realize-lib/src/storage/config.rs): `ArenaConfig` for storage paths.
    *   [`storage/real.rs`](crate/realize-lib/src/storage/real.rs): `LocalStorage` and `PathResolver` for "Real" files.
    *   (Future: `storage/unreal.rs` for `redb` based cache, `storage/history.rs`)
*   **`logic/`**: Contains the core synchronization algorithms and state management.
    *   [`logic/consensus/movedirs.rs`](crate/realize-lib/src/logic/consensus/movedirs.rs): Implements the initial "move directories" sync algorithm.
    *   (Future: `logic/consensus/*` for broader consensus, `logic/forest/*` for unified tree view, `logic/household/*` for peer management)
*   **`errors.rs`**: Defines `RealizeError` and other error types.
*   **`utils/`**: Utility functions.
    *   [`utils/async_utils.rs`](crate/realize-lib/src/utils/async_utils.rs): Async helper like `AbortOnDrop`.
    *   [`utils/hash.rs`](crate/realize-lib/src/utils/hash.rs): BLAKE2b-256 hashing utilities.
    *   [`utils/logging.rs`](crate/realize-lib/src/utils/logging.rs): Logging setup.

### Executable Crates

*   **`crate/realize-cmd/`**: Command-line interface tool (`realize`) for initiating sync operations and managing the system.
*   **`crate/realize-daemon/`**: Background daemon (`realized`) that runs on each peer, hosting the `RealizeService` and managing local storage/cache.

## Key Technical Decisions & Design Patterns

*   **RPC Framework:** `tarpc` for efficient, typed, asynchronous RPC between peers.
*   **Differential Sync:** `fast_rsync` principles (signatures and deltas) for minimizing data transfer for updates.
*   **Security:** TLS 1.3 with raw ED25519 public key verification (no CAs) for secure peer-to-peer channels.
*   **Local Cache (`Unreal`):** `redb` key-value store for metadata and blob storage for cached file content. Merkle trees for blob hashing.
*   **Filesystem Presentation:**
    *   NFSv3 server (via `nfsserve`) for cross-platform access to the "Unreal" cache (initially read-only).
    *   Kernel OverlayFS (Linux-only) for merging "Real" (local writable) and "Unreal" (remote read-only) views.
*   **Conflict Resolution:** Initial strategy is "don't resolve conflicts," keeping local modifications local and providing remote modifications as alternatives. A "conflicts" subdirectory is planned.
*   **Asynchronous Operations:** Extensive use of `tokio` for all I/O and concurrent operations.
*   **Configuration:** TOML files for daemon and peer household configuration.
*   **Modularity:** Clear separation of concerns into `model`, `network`, `storage`, and `logic` modules within `realize-lib`.

## Critical Implementation Paths

*   **`RealizeService` Implementation:** The server-side logic in [`network/rpc/realize/server.rs`](crate/realize-lib/src/network/rpc/realize/server.rs) is central to all peer interactions.
*   **`movedirs.rs` Sync Algorithm:** The core logic for the initial file synchronization mechanism.
*   **Storage Interaction:** How `RealizeService` methods interact with `LocalStorage` ([`storage/real.rs`](crate/realize-lib/src/storage/real.rs)) and eventually the "Unreal" cache.
*   **TLS Handshake and Peer Verification:** Critical for secure communication, implemented in [`network/security.rs`](crate/realize-lib/src/network/security.rs) and used by [`network/tcp.rs`](crate/realize-lib/src/network/tcp.rs).
*   **Partial File Handling:** Management of `.partial` files during transfers and their finalization.