# The Unreal - A partial local cache of remote files as FS

## Overview

Export a filesystem using
[nfsserve](https://github.com/xetdata/nfsserve) based on a
[redb](https://github.com/cberner/redb/tree/master) database and local
(blob) storage.

The filesystem works as a cache for remote data. It caches all files
available on remote peers and makes them visible through the FS. When
a file is accessed, it is downloaded and added into the cache.

Initial implementation is read-only and rely on another filesystem
storing any local changes. See [The Real](real.md).

This will be extended later into a read-write implementation that
stores local modifications for MacOS and Windows support.

## Details

The Unreal cache is built upon a `redb` database that tracks the state
of files across a household of peers. This allows the system to
maintain a local understanding of remote file availability without
constant network communication.

The initial implementation focuses on tracking file paths and their
state. The more advanced blob-based storage with Merkle trees will be
built on top of this foundation.

File hierarchy and path assignment is never evicted from the cache.
There must be enough place to store file hierarchy from all peers.
Cache eviction only applies to file data, which isn't described yet.

File hierarchy starts with a list of arenas, treating arenas whose
name has a slash as a directory. So given a server with the arenas:
movies, docs/office, docs/official, the root will contain [movies,
docs], docs will contain [office, official].

Note that, for this reason, it is an error for an arena name plus / to
be the prefix of another arena; this is checked at startup, when the
config file is loaded. For example, you couldn't have an arena called
"docs" and another called "docs/office", but you could have an arena
called "docs" and another called "docs_office".

### Access patterns

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

2. writing from remote peer data (through RealizeService::list and
   HistoryService::notify)

  - a list of (peer, arena, path, size, mtime) (in the future (peer,
    arena, path, blob id) + (peer, blob id, blob content))

  - as notifications come in (Notification::Unlink and
    Notification::Link from HistoryService), the internal state must
    be updated

This is common and should be done efficiently enough. Efficiency is
less of a priority for this access pattern than for access pattern 1
because it is run in the background, in batch operations.

3. after a long disconnection, data is re-read through
   RealizeService::list and compared with content

  - go through the entire content known from a peer, compare with
    RealizeService::list output then apply modifications

This should not happen often in the final design, as peers must store
history between disconnections to serve to other peers. Additionally,
this is typically run in the background.

4. whole arenas can be deleted

This must be possible but doesn't need to be efficient.

### Interface

The `UnrealCache` type exposes the following methods, corresponding to
the access patterns above. Each method is described in its own section
below.

```
// Access pattern 1:

fn readdir(inode) -> anyhow::Result<Iterator<ReadDirEntry>>;
fn lookup(inode, name) -> anyhow::Result<Option<ReadDirEntry>>;

// Access pattern 2:

fn link(peer, arena, path, size, mtime) -> anyhow::Result<()>;
fn unlink(peer, arena, path, mtime) -> anyhow::Result<()>;

// Access pattern 3:

/// Mark all files belonging to a specific peer.
fn mark_peer_files(peer) -> anyhow::Result<()>;

/// Delete marked files belonging to a specific peer that
/// have not been refreshed by a link() call since mark_peer_files.
fn delete_marked_files(peer) -> anyhow::Result<()>;

// Access pattern 4:

fn delete_arena(arena) ->anyhow::Result<()>;

```

Code location: crate/realize-lib/src/storage/unreal.rs

See definition of `ReadDirEntry`, `FileEntry` and `ConsensusDecision`
in the next section. Initially, the type that is stored is the type
that is returned, even though they'll likely diverge later.

Note that this proposal lacks a "read" call. This is TBD in a later
step.

Note that this proposal uses non-async (blocking) functions, since
redb is blocking. An async wrapper that runs tokio::spawn_blocking
might be necessary to call conveniently from the rest of the code.

#### readdir

This corresponds to the operations readdir and readdirplus of NFS and
FUSE: return the entries of a directory, optionally with metadata:
 - ownership and protection, which are hardcoded
 - size for file, a fixed size (1 block) for directories
 - mtime, coming from the file entry for file

 OPEN ISSUE: What mtime value to report for directories?

 OPEN ISSUE: Is it worth storing a copy of the metadata in the
 directory entry, to speed things up? Profile and add if necessary.

#### lookup

Look up the metadata of one one entry in a directory. The returned
metadata is the same as readdir.

#### link

Add a file for a peer.

If the same file already exist for that peer, use the file with the
highest mtime.

OPEN ISSUE: is a table for path -> inode needed to avoid having to
traverse the hierarchy for path-based operations ? This is an
optimization, to be added later after profiling, if necessary.

#### unlink

Remove a file for a peer.

If a file exists for the same peer with a higher mtime, ignore that unlink.

If no file exists yet, for the peer, ignore the call.

NOTE: It's the responsibility of the caller to deal with the case
where unlink comes before link for a specific file (unlink mtime >=
link mtime) because data for the race condition needs to be kept but
only for a short time and only in specific conditions; this is not
something that is worth storing.

#### mark_peer_files/delete_marked_files

This is meant to be used together with mark_peer_files when
a peer reconnects, to delete any file not mentioned again:

1. call mark_peer_files
2. call link for all files provided by the peer
3. when file list is complete, delete_marked_files

This requires link() overwriting the old content and removing the mark.

OPEN ISSUE: With the proposed database schema, this require going
through all files twice, first to mark, then to unmark. Is this
efficient enough? Probably, for a background call. Worse case
scenario, a file deleted on the remote peer says for longer than
strictly necessary. Profile and add an index if necessary.

#### delete_arena(arena)

This works exactly like recursive directory deletion, since arenas are
just directories in the proposed design.

### Database Schema

File hierarchy should be stored as a filesystem would, so it can be
served quickly for the access pattern that matters the most
(filesystem access).

Note that there's no inode table. Inodes should be looked up either as
a file or as a directory.

Everything starts with inode 1, which is always a directory, though
possibly empty.

New inode values are allocated sequentially, using
`max(directory_table.last().key.0, file_table.last().key.0, 1) + 1`.

This requires making sure that last() is really the highest numbered
inode. If necessary, the key and key serialization is to be
configured so that lexicographical order = natural order.

Inodes aren't reused. If we reach the max value for u64, new files or
dirs cannot be created anymore. It's hard to justify anything more
complicated. It's possible to switch to another scheme later in a
backward-compatible way.

**Directory Table**

Key: `(u64, String)` (directory inode, name)
Value:

```rust
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
  /// This is stored here as a key to fetch file content,
  /// to be replaced by a blob id.
  arena: model::Arena,

  /// The path to use to fetch file content in the peer.
  ///
  /// Note that it shouldn't matter whether the path
  /// here matches the path which led to this file. This
  /// is to be treated as a key for downloading and nothing else..
  ///
  /// This is stored here as a key to fetch file content,
  /// to be replaced by a blob id.
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

NOTE: This "on the fly" conflict resolution is good enough for a
start. A more configurable conflict resolution mechanism will have to
be built later on.

## Future: Blob Storage (Recap)

While not part of the initial implementation step, the database will
be extended to support blob-based storage:

*   **Blobs Table**: `hash -> reference count + (set of blob
    hash)|(path of block)`
*   **Merkle Trees**: File hashes will be constructed as Merkle trees
    to efficiently verify partial content.
*   **Block Storage**: Individual file blocks will be stored on disk,
    named by their hash.

This phased approach allows for an incremental build-out of the Unreal
cache, starting with the essential file tracking mechanism.
