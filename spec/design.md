# Realize As a Synced Filesystem

## Overview

The overall goal is to sync files partially between hosts. Some hosts
may store some files, other hosts other files. Some hosts may not
store any file permanently at all, and just serve them from a cache.
The choice can be made on a file by file or directory by directory
basis on every host.

On a given peer, the result would be represented as a filesystem with
2 layers:

 - the Real, that is, the files that are available locally and
   complete, stored on fs as file

 - the Unreal, that is, the files that are not completely available
   locally, kept in a key-value store and served directly from it.

Each peer keeps track of local Real and Unreal data. Some peers might
want to keep a copy of all files in the Real, other might want to just
cache the files that are needed in the Unreal and realize them on
demand only.

Peers keep a local history of local modifications (create, delete,
rename, edit, overwrite) to be sent to other peers when they're
connected. History entries are ordered by time. History entries at or
before T are deleted once all peers have been made aware of the local
changes.

If history grows too large because of a lagging peer, that peer can be
marked inactive and older history entries deleted. Previously inactive
peers are treated as new peers.

Extra operations are available on the files and directories of the
filesystem to mark them as :

  - Real: the file or directory content must be kept locally

  - Unreal: the file or directory content must be kept in the cache.
    New, edited or overwritten files are still kept in the real for a
    while but they'll move back to the unreal after Consensus.

New files share the setting of the containing directory. That setting
is only kept in the Real.

### Concepts

  - *The Real* files available locally and their directories.

  - *The Unreal* Remote paths, copied locally and blobs not
    available or not completely available locally

  - *An Arena* a set of path and their assignment to blobs. Note that
    directories only exist as path elements in the Unreal.

  - *Realize* put data from the Unreal into a local file

  - *Unrealize* move a local file to cache and let cache eviction
    strategy deal with it

  - *The Household* set of peers that sync some arenas of data. Some peers
    might only be interested in a subset of the Household's arenas.

  - *A peer* member of the Houshold, known by its public key, with which
    communication is possible.

  - *A listening peer* a peer with a known, stable address that can be
    connected to.

  - *A deaf peer* a peer that's not listening to a known address. It
    cannot be connected to. (Deaf peers can export services to peers
    they connect to.)

  - *A Blob* a piece of data, identified by its hash. A blob can be
    based on (edit) another blob, using byte-based patches (Blob B =
    Blob A + patch)

  - *A conflict* when two peers have different blobs assigned to the
    same path.

  - *Delete* unlink a blob from its path

  - *Rename* unlink a blob from path A, link it to path B

  - *Change* unlink the old blob from a path, link a new blob to that
    path. Note that the new blob might be based on the old blob.

  - *Edit* when a blob is based on another blob, it is an edit of the
    old blob.

  - *Create* link a blob to a path

  - *Consensus* is achieved for an arena when all peers agree on the
    set of paths the blob each path is assigned to.

  - *Reaching Consensus* is the process of two peers sending their
    changes and agreeing on the set of paths and the assigned blobs.

  - *Conflict* is when two peers want to assign a different blob, or
    no blobs at all, to the same path

## Details

### The Unreal

The Unreal contains remotely-available files, with, for each peer:

 - the peer id

- the address to connect to (unless the peer is the one to connect)

- a list Arenas, identified by their name

- for each Arena, list of remotely-available files (path) and their
  size (+more information? what about duplicates?)

While connected, the local server gets notified whenever files are
added, removed or changed, with a delay to make sure file is stable.
This way, the file list is always available locally. Notifications are
rate-limited and batched in case of heavy load.

While disconnected, the local server can still serve the last known
list of files, and the cached files. A batch of all history change is
sent to a peer upon connection.

Making change a file that's part of the Unreal moves it into the Real,
like when deleting, renaming, overwriting or editing.

File data that's part of the Unreal works as a cache: data that's
worked on often is kept in cache, older data is evicted as needed to
make space. The Real and the Unreal of an Arena compete for space, that
is cache data might need to be removed to allow a Real file to be
added. Real files take priority over cache content. This means that
the cache can be filled up to the quota allocated for real files and
cache data, as cache data can always be evicted to make space for real
files when necessary.

When multiple arenas are supported, two modes are possible:

 - different arenas have shared quotas and caches; they must be
   on the same filesystem for this to make sense

 - different arenas have separate quotas and caches

Initial cache eviction stategy:
 - LRU with priorities:
   1. delete unlinked blob data (starting with least recently used)
   2. delete linked blob data (starting with least recently used)
Manual commands allow the cache to be cleared or shrunk.

Detailled in [The Unreal](unreal.md)

### The Real

The Real contains locally-available files, split by Arena, as well as
local changes made to remote files.

For each Arena, a history of file addition, edit, overwrite, move and
deletion that are yet to be sent to peers. History sections are
removed once it's been sent to all known peers.

Detailled in [The Real](real.md)

### Real → Unreal

When a file is added to the Real, it is stored locally until Consensus
time, that is, the time where two peers sync their local changes.

Consensus time can be:

 - on a schedule

 - immediately, after a change for some operations (delete, rename)

 - after a rest period, for some operations (file creation, overwrite,
   edit)

After consensus has been reached, a file that was Real can become
Unreal, depending on the local settings and cache size.

#### Conflict resolution

Possible resolutions:

 - Don't resolve conflicts at all, keep local modifications local and
 provide remote modifications as alternatives

 - Resolve conflicts automatically and silently by applying a rule
 such as "most recent wins" (this unfortunately relies on local clocks
 being set correctly)

Whatever the solution, conflicting versions should be available,
somehow, some ideas:

 - Keep a "conflicts" subdirectory at the root, next to the arenas.
 Each conflicting file (arena name + path) is a directory with all
 versions available inside as peer + hash + ext, with their
 modification time set.

 - Provide a list of conflicts and access to the different versions in
 a separate management interface.

In addition, it would be possible to prevent a file from being edited
locally - even a real file - until the change has been downloaded if a
remote modification is known to exist. If the peer that made the
change is not connectable, protection would change to read-only and
(chmod +w) would override it. This could be a setting on real files.
Such a behavior would be too surprising to be a good default for a
file that's supposed to be local. It would fail rather easily,
unfortunately, unless peers tend to remain connected.

Unfortunately, this is a UI problem and filesystems don't really have
much on an UI.

Decision:

 1. start with "don't resolve conflict", so conflicting changes remain
    available.

 2. add a "conflicts" subdirectory to list conflicts and make
    alternatives available

 3. at this point, consider whether to switch to "most recent wins"

### Unreal → Real

When a file is marked as Real, it is fully downloaded into the cache.
Once fully available, it is realized, that is, turned into a file.

When a file is needed, it *might* be fully downloaded and turned into
a file, but only for a time (how long?)

## Milestones

1. Move files from one peer to the other, described and implemented in
   [Sync Algorithm](movedirs.md) (current as of 2025-05-29)

2. A read-only filesystem. Track remove file list locally, download when necessary. Detailed in [The Unreal](unreal.md)

3. The real: an overlay of a real filesystem and the read only filesystem created in step 2. Changes are stored
   in the real filesystem. Detailed in [The Real](real.md)

4. …

## Components and layers {#layers}

```
 OS/disk
   ▲
   │
   ▼
┌──────────────────────┬─────────────────────────────────────────┐
│ Realize Filesystem   │ Application and command─line tools      │  Interface layer
└──────────────────────┴─────────────────────────────────────────┘
  │
  │  ┌───────────────────────────────────────────────────────────┐
p │  │ The Fastness                                              │  Logic layer
a │  └───────────────────────────────────────────────────────────┘
s │
s │  ┌─────────────────────────────┐  ┌──────────────────────────┐
t │  │ Storage Layer               │  │ Network Layer            │  Data Layers
h │  └─────────────────────────────┘  └──────────────────────────┘
r │
u │         ┌────────────┬─────────┐  ┌────────────────┐
  │         │ The Unreal │ History │  │ RPC Services   │
  ▼         │   Cache    │         │  │                │
┌───────────┼────────────┴─────────┤  ├────────────────┴─────────┐
│ The Real  │   redb               │  │ Transport (TPC+TLS)      │
└───────────┴──────────────────────┘  └──────────────────────────┘

^^^^^^^^^^^^^^ (disk) ^^^^^^^^^^^^^^  ^^^^^^^^ (network) ^^^^^^^^^

```

### Source layout {#layout}

```
crate/
│                                  ┌─
│                                  │ Interface Layer
│                                  │
├── realize─cmd/                   │     Command-line utility
│                                  │
├── realize─daemon/                │     Background process (RPC + Fuse)
│                                  │
├── realize─fs/                    │     Fuse-specific code
│                                  └─
└── realize─lib/
    │
    └─ src/
       │                           ┌─
       ├─ logic/                   │ Logic Layer
       │  │                        │
       │  ├─ emissaries/           │     External-facing service impl
       │  │                        │
       │  ├─ forest/               │     Consolidated real/unreal tree view
       │  │                        │       and blob assignment
       │  │                        │
       │  ├─ consensus/            │     Bridging local and remote data
       │  │                        │
       │  └─ houshold/             │     Remote peer definition and state
       │                           └─
       │
       │                           ┌─
       ├─ network/                 │ Network Layer
       │  │                        │
       │  ├─ rpc/                  │     TARPC service definitions
       │  │  │                     │
       │  │  ├─ <servicename>.rs   │
       │  │  …  │                  │
       │  │     └─ server.rs       │
       │  ├─ tcp.rs                │     TCP transport for TARPC
       │  │                        │
       │  ├─ reconnect.rs          │     TCP service clients
       │  │                        │
       │  └─ security.rs           │     TLS and authorization (peer list)
       │                           └─
       │
       │                           ┌─
       ├─ storage/                 │ Storage Layer
       │  │                        │
       │  ├─ real/                 │     Data stored locally as files
       │  │                        │
       │  ├─ unreal/               │     Cached remote data
       │  │                        │
       │  └─ history/              │     Local change history
       │                           └─
       │
       ├─ model/                   Types shared across layers
       │
       └─ util/                    Random layer-agnostic utilities

```

## Choices

### Key-value store

Decision: redb. See [The Unreal](unreal.md) for details.

1. [redb](https://github.com/cberner/redb/tree/master)

   - 👍 Maintained
   - 👍 Simple, good performance
   - 👍 Crash-safe
   - 👍 Designed for a read-heavy workload
   - 👍 Type-safe
   - 👍 Reasonably stable

2. [sled](https://github.com/spacejam/sled)

   - 👍 Maintained
   - 🤦 Being rewritten
   - 👍 Powerful merge operators
   - 🤦 Regular fsync (every 500ms) for crash safetly
   - 🤦 Slightly worse performance than redb according to
     [benchmarks](https://github.com/cberner/redb/tree/master?tab=readme-ov-file#benchmarks)

### FUSE vs NFSv3 for remote file FS

Decision: NFSv3 (See [The Unreal](unreal.md) for the implementation,
[The Real](real.md) for the integration with the local filesystem)

FUSE is difficult to make work on MacOS, impossible on Windows. A
better cross-platform solution is use the approach of
[nfsserve](https://github.com/xetdata/nfsserve) and build a NFSv3
server, then bind it using existing OS tools.

FUSE

 - 👍 Powerful
 - 🤦 Complicated
 - 🤦 Linux-only, some support on MacOs, no support on Window

NFSv3

 - 🤦 Limited: no xattr, no file watch
 - 🤦 Some apps that are storage-based don't work well on NFS, but
   have no problems on a FUSE passthrough
 - 🤦 Anyone on the host can connect

Alternatives:

- https://github.com/Schille/bold-nfs?tab=readme-ov-file
- https://crates.io/crates/libnfs
-

### Overlay + passthrough

Decision: Kernel Overlay FS (See [The Real](real.md) for details).
MacOS and Windows support to be implemented on top of the unreal
read-only NFSv3 filesystem (so more limited, but usable).

Overlay (unreal FS + real FS) and passthrough (real FS) can be
implemented on Linux using Fuse or using the kernel's
[overlayfs](https://docs.kernel.org/filesystems/overlayfs.html)

1. Overlay FS [overlayfs](https://docs.kernel.org/filesystems/overlayfs.html)
   - 👍 Powerful and reliable
   - 👍 Well documented
   - 🤦 Linux only. While there's a chance of making FUSE work on
        MacOS, there's no chance of getting a kernel-based overlay fs outside
        of Linux.

2. FUSE using [fuse-backend-rs](https://github.com/cloud-hypervisor/fuse-backend-rs)
   - 👍 Maintained
   - 👍 Real usage
   - 👍 Passthrough + Overlay FS
   - 👍 Passthrough and Overlay FS support MacOS
   - 👍 Sync and async
   - 🤦 Uses raw C types everywhere (for passthrough)

3. FUSE using [fuse-rs](https://github.com/zargony/fuse-rs)
   - 👍 Good rust types everywhere
   - 👍 Easy to use
   - 👍 No Overlay FS, would need to be implemented
   - 🤦 Is it still maintained? Last commit was 5 years ago!

## Security

Peers of a household know all other peers by their public key.
Connections are made using TLS 1.3 with raw keys (ED25519). Both
client and server only accept connections made with a known peer key.

Cache, files and processes belong to the user who's running the
application and access is restricted to that used by the OS; no extra
security guarantees is given on top of what the OS provides. The
caches and the files should be stored in an encrypted filesystem by
the OS for better privacy.

Peers keeps a trimmed-down version of its history and of the history
it's been sent by other peers in text mode as an audit log, with
configurable storage location and retention.


