# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Blobstore {#blobstore}

Implement an Arena-specific blob store as described in the section
Blobstore of [unreal.md](spec/unreal.md).

Task List:
 1. Start with a blobstore with no expiration #blobstore1. Blobs are
    only ever deleted if it becomes outdated (the file is deleted or
    updated remotely.)
    See the section #blobstore1

 2. Hook it up with Downloader so it can serve locally available data
    and keeps any data that is downloaded for later.
    Details TBD

 3. Implement the Working Area as a LRU cache.

 4. Add a consistency/cleanup job that runs from time to time to
    compare file data with metadata in the database.

 5. Add TinyLFU accounting that gates the entry to a secondary LRU
    cache.

 6. Add a LRU cache for "to be deleted" data and batch job for
    deleting it.

## Blobstore 1st step: store files with no expiration {#blobstore}

Implement a first version of `open(inode, mode)` of [unreal.md](unreal.md).

Read the corresponding section of `unreal.md` as well as the
`Blobstore` section in the same file.

The relevant code and the file to modify is
[cache.rs](../crate/realize-storage/src/unreal/cache.rs)


Task list

 1. Have ArenaUnrealCacheBlocking take a directory in addition to a
    database. This is the directory where files will be stored. Update
    ArenaUnrealCacheBlocking::open, UnrealCacheAsync::open,
    UnrealCacheAsync::from_config and the configuration to pass these
    directories. In-memory caches used for testing, get /dev/null as
    directory, so any attempt to use them will fail.

    This is all in [cache.rs](../crate/realize-storage/src/unreal/cache.rs)

    Update all tests, including integration tests, run them with
    "cargo test"" and make sure they all pass.

 2. Add a `blobs` table (skip any fields that's for LRU accounting) and an
    entry `blob: Option<u64>` to FileContent. This table is described in
    the section Blobstore of [unreal.md](unreal.md)

    Run "cargo check -p realize-storage", then "cargo test -p
    realize-storage" and fix any issues.

 3. Add an `open` function to ArenaUnrealCacheConfig that just returns
    a `std::fs::File`. This requires creating a new entry in the `blobs` table,
    creating a file corresponding to that blob id and opening it.
    Expose the same function in UnrealCacheConfig that just delegates
    to it. UnrealCacheAsync gets a slightly different function that
    returns a `tokio::fs::File` created from the std file.

    Add unit tests for that function and the type it returns.

    Run "cargo check -p realize-storage", then "cargo test -p
    realize-storage" and fix any issues.

 4. Add code for deleting the blob whose id is stored in the
    FileTableEntry and delete its file when the default version (the
    entry without peer) changes or is removed (see calls to
    do_write_file_entry and do_rm_file_entry.).

    Add a test to check that.

    Run "cargo check -p realize-storage", then "cargo test -p
    realize-storage" and fix any issues.

 5. Add function to ArneaUnrealCacheConfig for getting and updating
    the ByteRange of a blob, the later only to be called after
    updating the file content flushing and syncing. Add the same
    functions to UnrealCacheConfig that just delegate to
    ArenaUnrealCacheConfig.

    Add a unit test that call the functions in UnrealCacheConfig and
    verify the ByteRange.

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

## Design: Multi-peer syncing {#multi-peer}

This needs more thoughts: do peer get told about non-local (indirect)
changes? do all peers need to be told about a non-local change?

What does it look like to add a new peer?

## Gate copy by file size {#sizegate}

Instead of allowing one file to copy at a time, allow multiple for a
total of up to CHUNK_SIZE bytes. A file > CHUNK_SIZE gets copied one
at a time, but smaller files can be grouped together. Might be worth
increasing the number of parallel files for that.

## IPV6 + IPV4 {#ipv64}

Localhost is currently resolved to ipv6 address, which isn't what's
expected in the tests, so all tests use 127.0.0.1.

This isn't right; it should be possible to specify localhost (or any
address that resolves to both an ipv6 address and an ipv4 address) and
have it work normally (try ipv6, fallback to ipv4).

This is normally automatic, I expect, but the custom transformation to
SocketAddr screws that up.

## Test retries {#testretries}

Make it possible to intercept RealStoreService operations in tests to
test:

- write errors later fixed by rsync
- rsync producing a bad chunk, handled as a copy later on

## Drop empty delta {#emptydelta}

Detect empty deltas from the rsync algorithm and skip sending a RPC
for them.

## Drop hash past dest filesize {#sizebeforehash}

Check dest filesize before sending a hash RPC and just store None in
the RangedHash.

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
