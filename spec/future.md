# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Turn Blobstore into a LRU cache {#bloblru}

Add a single-level LRU cache for the Working cache of the blobstore,
described in the section BlobStore of [unreal.md](unreal.md)

Blobs are added to the front of the LRU cache when they're created or
when they're opened. Blobs are removed from the LRU cache when they're
deleted.

All blob are added to the front of the LRU cache. open_file, in
[blob.rs](../crate/realize-storage/src/arena/blob.rs) moves the blob
to the front of the LRU cache.

The LRU cache keeps track of disk usage of the blob files, in bytes.
Since the blob files are sparse files, computing disk usage requires
computing `Metadata.block()*Metadata.blksize()` using
`std::os::unix::fs::MetadataEx`. Every time
extend_local_availability, defined in
[blob.rs](../crate/realize-storage/src/arena/blob.rs) updates a blob,
it updates the disk usage stored in `BlobTableEntry` and the total
disk usage of the containing LRU queue, stored in `QueueTableEntry`.

A new method on BlobStore, `cleanup(target_size)` removes entries from
the back of the LRU cache until the total `size <= target_size`.

Task list:

1. introduce a queue table, but don't use it yet

**blob_lru_queue**
Key: u16
Value: QueueTableEntry, defined in blobstore.capnp
 - first node :Option<BlobId>, BlobId is the key of the blob table
 - last node: Option<BlobId>
 - disk usage: u64 (in bytes)

The key of this table is LRU Queue ID, an enum defined in capnp:
 - working area (0)
 - protected area (1)
 - pending removal (2)

- define a type QueueTableEntry in [types.rs](../crate/realize-storage/src/arena/types.rs)
- define a corresponding capnp message in [blob.capnp](../crate/realize-storage/capnp/arena/blob.capnp)
- make QueueTableEntry implement the trait ByteConvertible and NamedType, like other types in `types.rs
- write a test case for the transformation of QueueTableEntry to and from bytes. See convert_indexed_file_table_entry in [types.rs](../crate/realize-storage/src/arena/types.rs) for an example
- make sure the code builds and the test pass
- define the LRU Queue ID enum in [blob.capnp](../crate/realize-storage/capnp/arena/blob.capnp)
- define the table in [arena.rs](../crate/realize-storage/src/arena/db.rs), add a getter to `ArenaWriteTransaction` and `ArenaReadTransaction`
- make sure the code builds

2. Extend `BlobTableEntry` to allow adding a blob to the queue (but don't add it yet)

- Add the following fields to `BlobTableEntry` in [types.rs](../crate/realize-storage/src/arena/types.rs):
 queue: u16 (queue ID enum, defined in the previous step)
 next: Option<BlobId>
 prev: Option<BlobId>
 disk_usage: u64 (disk usage in bytes)

- Do the same in the capnp representation of `BlobTableEntry` in
  [blob.capnp](../crate/realize-storage/capnp/arena/blob.capnp)

- Update the transformation from/into bytes of `BlobTableEntry` in [types.rs](../crate/realize-storage/src/arena/types.rs)

- Update and extend the unit test convert_table_entry to cover the new fields, make sure the new test passes

3. Keep all blobs in the queue (in the working area)

- Add a `BlobTableEntry` to the front of the queue when it is created
- Remove a `BlobTableEntry` to the front of the queue when it is removed
- Add a method `BlobStore::mark_accessed(BlobId)` that put a `BlobTableEntry` to the front of the queue if it's not already there.
- Write test cases and make sure they pass

4. Update disk usage count

- Update disk_usage in `BlobTableEntry` in extend_local_availability.
  Compute it in `Metadata.block()*Metadata.blksize()` using
  `std::os::unix::fs::MetadataEx`
- Apply any delta to the containing queue `QueueTableEntry` total disk
  usage
- Update total size in `QueueTableEntry` also when a blob is deleted
- Write test cases and make sure they pass

5. Add a method to `BlobStore::cleanup_cache(target)` that removes
   blobs from the back of the queue until the total size <= target.

- Write the method
- Write test cases and make sure they pass

TODO: define tasks for
 - calling mark_accessed in the background, after open_file from downloader.rs
 - configuring target size for the cache
 - calling cleanup_cache in the background, after cache size has increased or
   path marks have changed

## Expose connection info through RPC and display in realize-control {#conninfo}

## Add Mark::Ignore {#ignore}

Mark::Ignore means that a file or directory must not be indexed. It
might also mean that a file or directory must not be added into the
cache, even if reported by another peer.

## Update marks from xattrs {#marksxattrs}

Arenas, files and directories can be marked *own*, *watch* or *keep*
as described in the section Consensus of [real.md](real.md) and the
section Blobstore in [unreal.md](unreal.md)

The type `PathMarks` and `Mark`, defined in
[mark.rs](../crate/realize-storage/src/mark.rs) track that.

The goal of this task is to be able to add and remove paths from
`PathMarks` using file xattrs.

Extend `Watcher`, defined in
[watcher.rs](../crate/realize-storage/src/real/watcher.rs) to track
xattr `user.realize.mark` on files and directories and update
`PathMarks`, based on that.

The `Watcher` should be given a `PathMarks` type to modify. Typically
that type is empty with just the default (arena) mark set. It returns
a reference to that `PathMarks` instances from `Watcher::path_marks()
-> &PathMarks`

During catchup, the Watcher should just set marks as it finds them in
the `PathMarks` it's given without worrying about existing marks.

When receiving a notification, the watcher should set, update and
unset marks as xattrs change.

- Implement, then run `cargo check -p realize-storage --lib`, fix
  any issues.
- Add unit tests, then run `cargo test -p realize-storage marks`,
  fix any issues.
- Finally run `cargo test -p realize-storage`, fix any issues.

## Recover inode range in Arena cache {#inoderange}

When allocating a inode range in
[cache.rs](../crate/realize-storage/src/unreal/cache.rs), there are
two databases working together, the database of the global cache and
the database of the arena cache. Since 2-phase commit is not supported
between redb database, we can end up in a situation where the inode
range allocated for the arena by do_alloc_inode_range is forgotten by the
arena cache, as the transaction of the arena cache is aborted after
the transaction of the global cache has been submitted.

One solution to avoid that is to update the logic of
do_alloc_inode_cache so that if it's called by an arena cache that's
forgotten about an previously-allocated range, that
previously-allocated range is returned instead. It is possible, since
range only increase.

Task list:

- Have the arena cache pass the end of the exhausted range (or 0 the
  first time) eventually to do_alloc_inode_range

- before allocating a new range do_alloc_inode_range does a range
  search with the exhausted range `exhausted_range_end+1..` and if
  there is one range returned that's allocated to the current arena,
  return that.

This also allows reusing ranges if the entire arena database is deleted.

Task list:

 1. pass the end of the exhausted range from the arena cache to
    do_alloc_inode_range. Make sure the range compile.

 2. update do_alloc_inode_range to do a range check. Add a unit test.
    Make sure the test passes.

 3. add a test where all caches instances are dropped and re-created
    after deleting one of the arena cache, make sure the inode range
    of the deleted arena cache correspond to the previous range.


## Allow : in realize_types:Path {#colon}

Forbidding just brings trouble on Linux.

## IPV6 + IPV4 {#ipv64}

Localhost is currently resolved to ipv6 address, which isn't what's
expected in the tests, so all tests use 127.0.0.1.

This isn't right; it should be possible to specify localhost (or any
address that resolves to both an ipv6 address and an ipv4 address) and
have it work normally (try ipv6, fallback to ipv4).

This is normally automatic, I expect, but the custom transformation to
SocketAddr screws that up.

## Drop empty delta {#emptydelta}

Detect empty deltas from the rsync algorithm and skip sending a RPC
for them.

## Compression {#compress}

Implement compression as shown on:
https://raw.githubusercontent.com/google/tarpc/refs/heads/master/tarpc/examples/compression.rs

See whether it improves download. Currently we're at a surprisingly
stable 1.8MB/s when limited to 2MB/s (I assume 0.2MB/s for TLS)

This might not help much as long as the data is already compressed
(audio or video).

## Fix error message output {#errormsg}

When caught by with_context, error cause are printed.

When not caught by with_context, in move_files, error causes are not
printed. Also, in move_files, remote errors don't say which end (src
or dst) threw this.

- Fix error messages so that causes are printed. Keep error type cruft
  to a minimum.

- Add with_context to errors returned by a client (give a name to a
  client? use the address?)

- Print app errors in client at debug level
