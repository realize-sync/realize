# Refactoring ArenaStorage

Notes about reorganizing ArenaStorage into proper sub-types.

## Current Status

Currently, we have:

 - db (ArenaDatabase, in arena/db.rs)
 - dirty (DirtyPaths, in arena/engine.rs)
 - mark (in arena/mark.rs, integrated into ArenaCache)
 - index (in arena/mark.rs, integrated into ArenaCache
 - arena cache (in arena/arena_cache.rs)
 - blob (within arena cache)
 - watcher and hasher (on index+root)
 - realize/unrealize on (index+root+cache)
 - engine on (index+mark+dirty+cache)

everything uses the ArenaDatabase and sometimes the disk (for blobs and indexed files)

also, globally (in GlobalDatabase)):

 - global cache
 - inode allocator

external interfaces:

 - global cache -> arena cache (FS)
 - set/get/clear marks (through control)
 - engine job stream and job status (churten)
 - availability/get blob/update availability (download and churten)
 - realize/unrealize (churten)
 - history, read from disk (RPC)

### Issues

- pretty much everything is blocking because of redb and redb transactions
- there's a need to share transactions between different subsystems, but we have to choose beteen  ReadTransaction vs WriteTransaction (no common transaction trait) or pass around tables
- hierarchy is shared between local, cache and marks, and use the same inodes but has different content
- looking things up by inode is cumbersome, but we do need to:
   - expose things as a filesystem (NFS or FUSE) which forces the use of inodes
   - deal with hierarchical systems (not only files, but also marks)
- building it for tests is a lot of work
- tasks are spawn but not tracked. They may or may not die when types are dropped.
- Arc is used everywhere; hardly any ownership left. Some types are never really dropped.
  - useful to pass references around
  - necessary when spawning - and we spawn at all levels so have Arc at all levels
- ArenaCache and Blobstore are so tightly coupled they may be the same thing; it's awkward
- Index is a bad name. A concept is missing.

## Ideas

- Clearly separate external interfaces from implementation. Use
  facades. [External Interfaces](#external)

- Keep everything blocking, use async for external interface only, in
  the facades. This is a property of the external interfaces

- Build a tree subsystem to allow views of the same tree
  (for index, marks, cache (overall and for each peer))
  [Tree](#tree)

- Go back to unreal for cache, just "The Unreal" and not "The Unreal Cache",
  and go back to real for index, "The Real", not "The Real Index"

- Systematically use
  [TaskTracker]((https://docs.rs/tokio-util/latest/tokio_util/task/task_tracker/struct.TaskTracker.html)
  and
  [CancellationToken]((https://docs.rs/tokio-util/latest/tokio_util/sync/struct.CancellationToken.html)
  for background tasks *in the facade(s)*

### External Interfaces {#external}

Storage is the facade. Review what is exposed, directly or indirectly.
Storage is where async is added and where background tasks are started.

#### global cache -> arena cache (FS)

 - define a trait in global, expose that trait on ArenaCache
 - don't include anything not strictly needed to implement the FS layer and download in Churten from that trait
 - includes availability/get blob/update availability/mark verified (for download)
 - the trait is blocking, async is provided by the global cache.
 - the global cache is in effect the facade for the FS layer to use
 - move types shared by global cache up (such as Blob) to mark them as external, extract
   traits as needed

#### get/set/clear marks

 - in Storage, move mark up to mark it as external
 - delegate to arena-specific type (?)

#### engine job stream and job status (or should it be download/download status?)

 - in Storage, move Job, JobStatus up to mark it as external

#### realize/unrealize (maybe not external)

 - in Storage
 - delegate to arena-specific type (?)
 - should they be made internal or not? Unrealize needs nothing from core and realize
   could be just a follow-up to download that triggers when a blob is verified that
   would reduce the external interface and limit Churten to download

#### history, read from disk (RPC)

 - history is not necessarily limited to the Real; should be extracted

 - read as well should be designed as something separate from the Real
   that could be applied to blob/blob file

### Tree {#tree}

A type that keeps track of the directory/file tree, but not what it
corresponds to. It map names to ???, let other subsystem link that to
whatever they like. No deletion, once a name mapping is there.

??? is depends on the subsystem/layer. Ideally it's opaque to the
Tree subsystem that just maps names to these opaque things.

Initially and in the current implementation ??? is an inode, but that
might not be a good idea if we want to support rename and hard links.
(Maybe: do map names to inodes in a stable way, but have a separate
mapping table for renames and hard links)

The Tree subsystem allows merging arbitrary layers, going through them
at the same time.

Takes transactions to allow merging into larger operations. (Problem:
read vs write transactions; no ReadableTransaction)

### History {#history}

Take history out of the ArenaCache and generalize it.

Interfaces:
 - an internal interface for other types to add to the history and report
   changes.

   Takes transactions to allow merging into larger operations.
   (Problem: read vs write transactions; no ReadableTransaction)

 - an external interface for letting remote peers know about changes

 - an external interface for downloading files (or blobs, see
   [Blobstore](#blobstore))

### Blobstore {#blobstore}

Make a blobstore as initially envisioned. The current compromise is awkward.

Have:

u64 -> hash + size + mtime +blob data + refcount
hash -> u64

then just store u64 instead of hashes and track with refcounts (weak
and strong for blob data "working" vs "protected")

The current implementation tried to avoid the duplication of hashes in
the two indexes, but actually ends up storing them more than twice.

This means that blobstore should be usable by both the index and the
cache, to resolve hashes to the same local ids, possibly to point
hashes to local files.
