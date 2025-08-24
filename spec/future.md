# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Call updatedb regularly while downloading {#updatedb}

Currently, Blob::updatedb is called only after closing the file,
possibly losing a lot of data and delaying cache eviction. Call
Blob::updatedb regularly to avoid that; every MB for example.

## Attempt cleanup when blob is closed {#cleanup}

Currently, cleanup runs when disk usage changes, but cannot delete
open blobs, meaning that very often it'll leave possibly large files
that used to be open until some other disk usage changes.

## realize-control mark get should return arena mark {#getarena}

Currently realize-control mark get expects files and since there
aren't any, returns nothing. That's not useful.

## Make config file nicer {#configfile}

Config file should be able to take units for byte fields. disk_usage
format should be nicer, it could, for example, use suffixes to choose
the unit (bytes if nothing, percentage if %, bytes if MB, G, ...)

## Make blobs directory at startup or require it to exist {#blobdir}

Without it, disk_usage fails.

## Re-design churten {#nochurten}

With the latest changes, churten doesn't make much sense anymore; it's
just download. Also, important information is missing such as:

- initial hashing, which could take a while
- download/verify/evict
- realize/unrealize
- connect/disconnect
- local changes caused by remote notifications (out-of-date, deletion)

There might be a need for an "audit" concept to log some of these.

Possibly use or integrate with tracing.

## Reconsider needing to hash before adding file {#musthash}

What if "no version" files could be reported? Remote FS could be up
and ready quicker, though without version.

## Investigate why only one file is downloaded at a time {#onefile}

Churten downloads one file at a time, instead of 4. 4 are displayed,
but only one progresses.

## Trim history {#trimhistory}

Decide on rules for trimming history.

## It should be possible to read a large file without storing it {#largedl}

Currently, when accessing remote files, the data is always downloaded.
If the file is kept as long as it's open, even if it's too large to
fit in the working area. That's wrong.

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
