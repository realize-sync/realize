# The Unreal - A partial local cache of remote files as FS

## Overview

Export a filesystem using
[nfsserve](https://github.com/xetdata/nfsserve) based on a
[redb](https://github.com/cberner/redb/tree/master) database and local
storage.

The filesystem works as a cache for remote data. It caches the file
hierarchy available on remote peers and makes them visible through the
FS. When a file is accessed or when the user asks for it, the file is
downloaded and added into the cache.

This relies on another, Linux-only, filesystem storing local data and
sending notifications about changes, [The Real](real.md).

Initial implementation is read-only. This will be extended later into
a read-write implementation that stores local modifications for MacOS
and Windows support.

## Stages

Design and implementation happens in stages, incrementally:

1. Store file hierarchy, described and implemented.
2. Serve data from network, described and implemented.
3. Store file content, in an expiring cache, see [Future].

## Details

The Unreal cache is built upon a `redb` database that tracks the state
of files across a set of peers, called the Household. This allows the
system to maintain a local understanding of remote file availability
without constant network communication.

The initial implementation described here tracks file paths and their
last modification time. The more advanced storage, versioned with
hashes will be built on top of this foundation.

File hierarchy and path assignment is never evicted from the cache.
There must be enough place to store file hierarchy from all peers.
Cache eviction only applies to file data.

File hierarchy starts with a list of arenas, a named collection of
files, which are represented as directories sitting just under root.
The files containing within each arena are put under the arena's
directory. So the file "foo/bar" in the arena "test" is represented as
"/test/foo/bar". Arenas names can have slashes in them, so might be
represented by more than one directory; The file "blurg" in the arena
"documents/office" is represented as "/documents/office/blurg"

### Arenas

Arenas are separate collections of files, kept in the same cache.
They're part of the startup configuration. They're passed to the cache
when it is created.

The cache create initial directories for each configured arenas. These
directories are called "Arena roots" and treated specially by the
cache.

Since arena names can contain slashes, an arena can be represented by
a hierarchy of directories.

To avoid mixing arenas roots with arena content in the same directory,
it is an error for an arena name plus / to be the prefix of another
arena; this is checked at startup, when arenas are created. For
example, you couldn't have an arena called "docs" and another called
"docs/office", but you could have an arena called "docs" and another
called "docs_office".

### Access patterns

The design described below is meant to meet the needs of the following
expected access patterns.

1. as a filesystem

- list and read as a filesystem, that is:
  - file content is access identified by an inode (u64)
  - directories are identified by an inode (u64). They contain a list
    of inodes (files and directories)
    - they can be read fully (readdir)
    - entries with specific name can be looked up (lookup)
  - everything is accessed layer after layer: reading the file a/b/d,
    meaning looking up "a" to get its inode, then looking up "b" in
    "a" by its inode then looking up "d" in "b" by its inode, then use
    the inode of "d" to get the file content)
- it all starts from a well-known root inode, pointing to the root
  directory .

This is very common and typically done in the foreground, with the
user waiting for the result. It must be done efficiently

2. writing from remote peer hierarchy notification (through
   RealStoreService::list and HistoryService::notify)

  - a list of (peer, arena, path, size, mtime) and, in the future
    (peer, arena, path, size, mtime, hash)

  - as notifications come in (Notification::Unlink and
    Notification::Link from HistoryService), the internal state must
    be updated

This is common and should be done efficiently enough. Efficiency is
less of a priority for this access pattern than for access pattern 1
because it is run in the background, in batch operations and redb
allows concurrent read access while those modifications are being
applied.

3. after a long disconnection, data from a remote peer is (re-)reported
   in its entirety and compared with the current content for that peer.

  - add entries that are reported into the cache, allowing re-reporting
  - go through the entire content known from a peer to remove entries
    that where not reported again.

Efficiency is not a priority, as this is done in the background and
read access is allowed while the cache is being modified.

The [overall design](../design.md) calls for peers storing history
between disconnection to serve other peers, so this should be a rather
uncommon operation.

4. whole arenas can be renamed or deleted

This must be possible but doesn't need to be efficient.

### Interface

The `UnrealCacheBlocking` type exposes the following methods, corresponding to
the access patterns above. Each method is described in its own section
below.

```
// Access pattern 1:

fn readdir(inode) -> anyhow::Result<Iterator<ReadDirEntry>, UnrealCacheError>;
fn lookup(inode, name) -> anyhow::Result<Option<ReadDirEntry>, UnrealCacheError>;
fn file_metadata(inode) -> anyhow::Result<FileMetadata, UnrealCacheError>;
fn dir_mtime(inode) -> anyhow::result<SystemTime, UnrealCacheError>;
fn read(inode) -> anyhow::Result<impl AsyncRead+AsyncSeek, UnrealCacheError>;

// Access pattern 2:

fn link(peer, arena, path, size, mtime) -> anyhow::Result<(), UnrealCacheError>;
fn unlink(peer, arena, path, mtime) -> anyhow::Result<(), UnrealCacheError>;

// Access pattern 3:

/// Mark all files belonging to a specific peer.
fn mark_peer_files(peer) -> anyhow::Result<(), UnrealCacheError>;

/// Delete marked files belonging to a specific peer that
/// have not been refreshed by a link() call since mark_peer_files.
fn delete_marked_files(peer) -> anyhow::Result<(), UnrealCacheError>;

// Access pattern 4:

fn delete_arena(arena) ->anyhow::Result<(), UnrealCacheError>;

```

See definition of `ReadDirEntry`, and `FileEntry` in the next section.
Initially, the type that is stored is the type that is returned, even
though they'll likely diverge later.

`UnrealCacheError` is an error implemented with `thiserror` that
collects the different error variants, so they can be transformed into
`std::io::Error` when implementing the filesystem. Define custom
errors when not forwarding another error. Write a From when forwarding
a redb error.

These are non-async (blocking) functions, since redb is blocking. An
async wrapper (UnrealCacheAsync) that just runs the methods on
tokio::spawn_blocking will be necessary to make calling these function
conveniently from the async parts of the code.

Code location:
 - crate/realize-lib/src/storage/unreal/cache.rs
 - crate/realize-lib/src/storage/unreal/error.rs
 - crate/realize-lib/src/storage/unreal/sync.rs
 - crate/realize-lib/src/storage/unreal/future.rs

Public types are exposed as being in realize-lib::storage::unreal.

#### readdir

This corresponds to the operations readdir and readdirplus of NFS and
FUSE: return the entries of a directory, with file attributes.

- ownership and protection (hardcoded)

- size for file, a fixed size (1 block) for directories

- mtime, coming from the file entry for file, local last modification
  for directories

 OPEN ISSUE: Is it worth storing a copy of the metadata in the
 directory entry, to speed things up? Profile and add if necessary.

#### lookup

Look up one entry in a directory and return its inode.

#### file_metadata dir_mtime

Return the file metadata or the last modification time of a directory.

#### link/catchup

Report availability of a remote file from a peer.

If the same file already exists for that peer, overwrite the entry if
the new mtime >= the old mtime.

If the same file already exists for another peer, keep both but serve
only the one with the highest mtime value. If both have the same mtime
value, either might be served.

`catchup` is a variant of link that is sent by a peer upon
reconnection for (re-)reporting files. In addition to linking the
file, catchup also removes the file deletion mark. See
`mark_peer_files`/`delete_marked_files` below.

OPEN ISSUE: is a table for path -> inode needed to avoid having to
traverse the hierarchy for path-based operations ? This is an
optimization, to be added later after profiling, if necessary.

#### unlink

Report a remote file becoming unavailable from a peer.

If a file exists for the same peer with a higher mtime, ignore the call.

If no file exists yet for the same peer, ignore the call.

If the same file still exists from other peer, the file is kept and
might be served from these other peers, otherwise it is removed from
the hierarchy.

A race condition is possible: what if `unlink` for a file is sent
before the `link` ? With the above algorithm, the `unlink` would be
lost. It's the responsibility of the caller to deal with this case. In
practice, this means that the peer must only send `unlink`
notifications after having reported all the `catchup` notifications.

#### mark_peer_files/delete_marked_files

This is meant to be used together with `mark_peer_files` when a peer
reconnects, to delete any file not mentioned again:

1. call `mark_peer_files`, which goes through the set of files from that peer and
   marks them for deletion
2. call `catchup` for all files provided by the peer
3. catchup ends, call `delete_marked_files`

This requires catchup removing the mark.

OPEN ISSUE: With the proposed database schema, this require going
through all files to mark them. Is this efficient enough? Probably, for
a background call. Worse case scenario, a file deleted on the remote
peer says for longer than strictly necessary. Profile and add an index
if necessary.

#### delete_arena(arena)

This works exactly like recursive directory deletion, since arenas are
just directories in the proposed design.

#### rename_arena(arena, new_name)

This updates the arena root and otherwise works like renaming
directories, since arenas are mapped as directories. The same restrictions
apply on the new name as for the initial set of arenas. See [Arenas]

#### read(inode)

The `read` method returns type that implements `AsyncRead` and
`AsyncSeek` from `tokio::io`, so it can be accessed with all the bells
and whistles through `AsyncReadExt` and `AsyncSeekExt`.

`read` chooses one of the peers that reported having the most recent
version of the file available and connects to it to retrieve it
through `RealStoreService`. It caches just one "chunk" of data,
between 8k and 32k.

Eventually, `read` will be able to serve partially or fully from the
database. See the [Future] section.

### Database Schema

File hierarchy should be stored as a filesystem would, so it can be
served quickly for the access pattern that matters the most
(filesystem access).

Inodes should be looked up either as a file or as a directory; there's
no global inode table.

Everything starts with inode 1, which is always a directory, though
possibly empty.

New inode values are allocated sequentially, using the single-entry
table `max_inode` which just stores the highest inode allocated so
far.

Inodes aren't reused. If we reach the max value for u64, new files or
dirs cannot be created anymore. It's hard to justify anything more
complicated. It's possible to switch to another scheme later in a
backward-compatible way.

**Directory Table**

Key: `(u64, String)` (directory inode, name)
Value:

```rust
enum DirtableEntry {
  Regular(ReadDirEntry),
  Dot(SystemTime), // A special entry named "."
}

struct ReadDirEntry {
  inode:u64

  assignment:InodeAssignment,
}
enum InodeAssigment {
  /// The inode of a file, look it up in the file table.
  File,
  /// The inode of a directory, look it up in the directory table.
  ///
  /// Note that an empty directory won't have any entries in
  /// the directory table.
  Directory,
}

```

A FS lookup operation would have the full key, but a FS readdir
operation would need to do a range lookup to collect all names.

**File Table**

Key: `(u64, String)` (file inode, peer)
Value:

```rust
struct FileEntry {
  /// The arena to use to fetch file content in the peer.
  ///
  /// This is stored here as a key to fetch file content.
  arena: model::Arena,

  /// The path to use to fetch file content in the peer.
  ///
  /// Note that it shouldn't matter whether the path
  /// here matches the path which led to this file. This
  /// is to be treated as a key for downloading.
  ///
  /// This is stored here as a key to fetch file content.
  path: model::Path,

  metadata:FileMetadata,

  /// File is marked for deletion.
  ///
  /// See mark_peer_files and delete_marked_files above.
  marked: bool,
}
struct FileMetadata {
  size:u64,
  mtime:SystemTime,
}
```

Looking up file content requires range lookup, as the same inode can
have data in more than one peers.

If data is available from more than one peer, choose the one with the
highest mtime. If more than one peer have the same maximum mtime, data
can be fetched from any of these.

If data is available from more than one arenas, choose the one from
the first arena, as declared in the config file.

NOTE: This "on the fly" conflict resolution is good enough for the
cache, which just reports what is available. A more configurable
conflict resolution happens when peers sync; this outside the scope of
the unreal cache.

**File Table**

Key: `(u64, String)` (file inode, peer)
Value:

## Future

These changes are planned and described in [the overall
design](./design.md) but not yet integrated into this design and its
implementation.

### Store File Content

In the future, the cache will store file content, so `read` can serve
directly from the cache, without connecting to a peer. For that to
work, everything read from a peer should be added to the cache.

File content is subject to cache eviction strategy (LRU), to make
room, if necessary.

Details TBD

### Versioned Storage

The database will be extended to support versioning of file content,
using their hashes. Hashes can be used to identify file content in the
database (blob storage).

Details TBD
