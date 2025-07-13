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

## Implementation Status

- IMPLEMENTED: Cache and Arena Cache
- NOT IMPLEMENTED: Blobstore

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

The cache architecture consists of two main types:

- `UnrealCacheBlocking`: The global cache that manages inode allocation and delegates operations to arena-specific caches
- `ArenaUnrealCacheBlocking`: Per-arena caches that handle file hierarchy and metadata for their specific arena

The `UnrealCacheBlocking` type exposes the following methods, corresponding to
the access patterns above. Each method is described in its own section
below. Most methods delegate to the appropriate `ArenaUnrealCacheBlocking` instance.

```
// Access pattern 1:

fn readdir(inode) -> anyhow::Result<Iterator<ReadDirEntry>, UnrealCacheError>;
fn lookup(inode, name) -> anyhow::Result<Option<ReadDirEntry>, UnrealCacheError>;
fn file_metadata(inode) -> anyhow::Result<FileMetadata, UnrealCacheError>;
fn dir_mtime(inode) -> anyhow::result<SystemTime, UnrealCacheError>;

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
 - crate/realize-storage/src/unreal/cache.rs (contains both UnrealCacheBlocking and ArenaUnrealCacheBlocking)
 - crate/realize-storage/src/unreal/error.rs
 - crate/realize-storage/src/unreal/sync.rs
 - crate/realize-storage/src/unreal/future.rs

Public types are exposed as being in realize-storage::unreal.

#### Cache Initialization

The `UnrealCacheBlocking::new` constructor takes a global database
path and a map of arena names to their database paths.

Each arena cache is initialized with its own database and inode range
allocation. The global cache maintains a hash map of
`ArenaUnrealCacheBlocking` instances, one per arena.

#### readdir

This corresponds to the operations readdir and readdirplus of NFS and
FUSE: return the entries of a directory, with file attributes.

The method first determines which arena the inode belongs to using
`lookup_arena(inode)`, then delegates to the appropriate
`ArenaUnrealCacheBlocking::readdir` method.

- ownership and protection (hardcoded)

- size for file, a fixed size (1 block) for directories

- mtime, coming from the file entry for file, local last modification
  for directories

 OPEN ISSUE: Is it worth storing a copy of the metadata in the
 directory entry, to speed things up? Profile and add if necessary.

#### lookup

Look up one entry in a directory and return its inode.

The method first determines which arena the directory inode belongs to
using `lookup_arena(inode)`, then delegates to the appropriate
`ArenaUnrealCacheBlocking::lookup` method.

#### file_metadata dir_mtime

Return the file metadata or the last modification time of a directory.

These methods first determine which arena the inode belongs to using
`lookup_arena(inode)`, then delegate to the appropriate
`ArenaUnrealCacheBlocking` methods.

#### link/catchup

Report availability of a remote file from a peer.

The method delegates to the appropriate
`ArenaUnrealCacheBlocking::link` or
`ArenaUnrealCacheBlocking::catchup` method based on the arena
parameter.

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

The method delegates to the appropriate
`ArenaUnrealCacheBlocking::unlink` method based on the arena
parameter.

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

These methods delegate to all `ArenaUnrealCacheBlocking` instances, as
files from a peer may be distributed across multiple arenas.

This requires catchup removing the mark.

OPEN ISSUE: With the proposed database schema, this require going
through all files to mark them. Is this efficient enough? Probably, for
a background call. Worse case scenario, a file deleted on the remote
peer says for longer than strictly necessary. Profile and add an index
if necessary.

#### delete_arena(arena)

This removes the arena from the global cache's arena map and deletes
the arena's cache instance. The arena's database files are also
removed.

This works exactly like recursive directory deletion, since arenas are
just directories in the proposed design.

#### rename_arena(arena, new_name)

This updates the arena root and otherwise works like renaming
directories, since arenas are mapped as directories. The same restrictions
apply on the new name as for the initial set of arenas. See [Arenas]

### Database Schema

The Unreal cache is split into a global cache and per-arena caches.
The global cache handles inode allocation and lookup across all
arenas, while each arena has its own cache for file hierarchy and
metadata.

#### Global Cache Database

The global cache manages inode allocation and provides arena lookup functionality.

**INODE_RANGE_ALLOCATION_TABLE**

Key: u64 (inode)
Value: &str (arena name, empty string "" for global inodes)

This table allocates increasing ranges of inodes to arenas. To find
which arena an inode belongs to, lookup the range [inode..]; the first
element returned is the end of the current range containing that
inode.

To allocate a new range for an arena:
1. Lookup the last element of the table (0 if absent), call it A
2. Add the number of inodes to allocate (e.g., 10000), call it B
3. Insert (B, arena) into the table
4. Return the range [A+1, B) to the arena

**CURRENT_INODE_RANGE_TABLE**

Key: &str (arena name)
Value: (u64, u64) (current inode, end of range)

Each arena tracks its current inode allocation within its assigned
range. When an arena needs a new inode:
1. Query its current range
2. If missing, request a new allocation from the global cache
3. If current < end, increment current and return the new inode
4. Otherwise, request a new allocation and update the range

#### Arena Cache Database

Each arena has its own database containing the file hierarchy and
metadata for that arena. The structure mirrors the original design but
is scoped to a single arena.

File hierarchy should be stored as a filesystem would, so it can be
served quickly for the access pattern that matters the most
(filesystem access).

Inodes should be looked up either as a file or as a directory; there's
no global inode table within an arena.

Everything starts with inode 1, which is always a directory, though
possibly empty.

New inode values are allocated from the arena's assigned range, using
the arena's CURRENT_INODE_RANGE_TABLE.

Inodes aren't reused. If we reach the max value for u64, new files or
dirs cannot be created anymore. It's hard to justify anything more
complicated. It's possible to switch to another scheme later in a
backward-compatible way.

**Directory Table** (per arena)

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

**File Table** (per arena)

Key: `(u64, &str)` (file inode, Option<peer>)
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

An entry with no peer is maintained in parallel with the peer entries
as the chosen version, the one shown in the directory and returned
when read.

This version is chosen in a way that tracks hash replacements:

1. When the 1st peer entry is added, add a no-peer entry with the same
   information.

2. Whenever Notification::Replace comes with old_hash == no-peer entry
   hash, update the no-peer entry with the new entry.

3. If there are no peer that has the version chosen in no-peer entry,
   choose another version among the available one, the one with the
   max mtime, and track than from now on.

With this algorithm, the version that's returned is the most recent one
according to one hash version chain, no matter what peer has it.

There might be more than one chain available in the peers. The cache
just chooses one (the first one added) and tracks that one, ignoring
any parallel chain.

Here's an example of a situation with two chains:

```
   / ----- B ---- D ---- E -----\
A -|                            |- G
   \--------- C -------------F--/

```

In this situation, the algorithm first chooses A, then moves on to B
(the first reported), D, E and G, completely ignoring C and F.

Choosing a different chain requires a UI that's not yet available.
Eventually, it should be possible for a cache to query the history
chain of all peers and to display that to the user for selection when
there is more than one chain.

Step 3 meant that the cache does jump history chains whenever a chain
ends (with a Remove(old_hash=current)) so that a file remains
available as long as it's available in at least one peer. This might
be questionable (the history chain we selected and tracked has indeed
ended), but practical (the user can get hold of a version of the
file.) Once there is a UI, this might be an option.

## Future

These changes are planned and described in [the overall
design](./design.md) but not yet integrated into this design and its
implementation.

### Store File Content

The cache stores file content, fully or partially.

Stored file content is useful:

- for working on a file locally, as the same data is often read more
  than once. Even partial data is useful as often utilities will read
  only a subset of a large video file to extract information about it;
  typically just the beginning and sometimes end, depending on the
  format.

- for reading files without being connected to the peer that owns them

In general, when downloading a file, the data should be stored
optimistically, then kept in a cache portion of the store for later
use. In addition to that, files can be tagged for download.

#### Blobstore

The blob store functions as a cache of file data. A cache size should
be set, either absolutely or as a percentage of the filesystem size.
Cache size may be reduced if filesystem usage is above a certain
percentage.

The cache stores blobs of different size, computed in term of blocks
(using the filesystem block size, gathered at first startup.)

When reading a file from the cache (*active file*), a sparse file of
the right size is created, then filled as the file is accessed. As
blocks are added to the file, files might need to be evicted from the
cache to make room.

When marking a file as available offline (*marked files*), the file is
downloaded in the background and added to the cache unconditionally.
As blocks are added to the file, files might need to be evicted from
the cache to make room.

The Blobstore is split into two areas: a working area and a protected
area, each working as a cache with LRU eviction policy taking a
percentage of the total cache size (such as 20/80). Files always start
in the working area, which works as a LRU cache. When evicted from the
Working area, marked files and files that are deemed useful enough are
moved to the protected area, which might need to evict some data to
make room, chosen using LRU.

Files that are deleted move into "Pending removal", which also works
as a LRU cache. When more space is needed, it is first taken by
evicting data from "Pending removal" then from "Protected". A
background process might also empty "Pending removal".


```
     ╷
     │ New
     ▼
╭──────────╮  (LRU)
│ Working  │╶───────▶ marked?
╰──────────╯ evict    frequently used?
     ╷                protected area not full? (maybe)
     │                    ╷
     │                    │ Add
     │                    │
     │                    ▼
     │             ╭────────────╮  (LRU)
     │             │ Protected  │╶────────▶ delete
     │             ╰─┬──────────╯  evict
     │               │
     │               │
     │               │  ╭─────────────────╮
     ╰───────────────┴─▶│ Pending Removal │╶───────▶ delete
                        ╰─────────────────╯ evict
```

This setup is freely inspired by "TinyLFU: A Highly Efficient Cache
Admission Policy" (Einziger, Friedman)

Figuring out usage frequency happens outside of the Blobstore, working
on inodes and their access patterns. See next section.

A Blobstore for an arena is a directory within that arena with:
 - a redb database "store.db"
 - set of files with a u64 as name, encoded as Base64 without final ==

The redb database stores the following tables:

**blob**
Key: u64 (incrementing)
value: BlobTableEntry, defined in crate/realize-storage/capnp/unreal/blobstore.capnp
 - owning inode (for getting frequency)
 - areas that have been written (ByteRanges)
 - used blocks, total blocks  (for comparison with FS, if needed)
 - ID of containing LRU queue (see queue table)
 - next and prev (key of blob table) for inclusion into a doubly-linked list for LRU

The file name is the key encoded as base64 no ==. Some periodic job
should compare files with database and fix any discrepancies. When
writing to both database and filesystem, operation order is chosen
carefully in case one step fails so work can continue where it left
off. For example:
 - Creation: 1. write to db, 2. create file 3. set used blocks/total blocks in db
 - Write data: 1. write data to file, 2. update range and block/total blocks in db
 - Delete: 1. delete file, 2. delete entry, decrease block/total block in db

When a new blob is added, a sparse file of the right size is created.
It is filled as data is downloaded. More on sparse files on Linux [Handling sparse files on Linux](https://www.systutorials.com/handling-sparse-files-on-linux/#lseek), MacOS [APFS How sparse files work](https://eclecticlight.co/2024/06/08/apfs-how-sparse-files-work/), Windows [DeviceIoControl
FSCTL_SET_SPARSE
IOCTL](https://learn.microsoft.com/en-us/windows/win32/api/winioctl/ni-winioctl-fsctl_set_sparse)

Used blocks/total blocks come from the filesystem (unix MetadataExt)
and are updated after each write. This is mainly used to know what to
decrease the cache size by if a file disappears.

When these values are increased or decreased, so is the block count of
the containing queue.

**queues**
Key: u16 (LRU Queue ID, an enum defined in blobstore.capnp: working area, protected area, pending removal)
Value: QueueTableEntry, defined in blobstore.capnp
 - first node (a key of the blob table)
 - last node
 - block count

The blob id is kept in the cache, associated with a specific version
of a file (hash). When that version is deleted or overwritten, the
blob is moved to the pending removal queue. It is not immediately
deleted, as existing data might be useful when downloading a new
version or to serve while offline, still pending removal have the
lowest priority priority and will be evicted whenever more space is
needed, before evicting from the protected area.

**settings**
Key: u16 (setting ID, an enum with u16 as repr, to be consistent with capnp enums)
Value: [u8] (bytes, depending on setting)

- block size, read when the database is created
- uuid, generated when the database is created

#### Computing Usage Frequency in the Cache

Each inode has the fields: usage count, generation. Each use (reading
1 block through download of store access) increases usage count until
a certain total usage value is reached (called W, TBD), after which
the existing counts are halved and generation is increased.

This is an implementation of usage count described in "TinyLFU: A
Highly Efficient Cache Admission Policy" (Einziger, Friedman) adapted
for a situation where we can keep data for all possible entries
(they're the inodes).

Keeping usage count and generation avoids having to update all entries
when generation increases. An entry that's not accessed for a long
time will have a usage count that's part of a very old generation,
that can be used to compute the latest usage count.

To avoid writing for every read access, uses are kept in memory as a
journal and dumped to database after a certain time, such as 5 or 10
minutes or when the process is shut down. This means that access
history might be lost.

A file is worth keeping if its value is >= (blocks*W/C), with C the
cache size is blocks and W the max usage value of a generation.

Note that since this relies on FS block size, frequency computation
might need to be updated or discarded if the FS block size of the
arena blobstore changes.
