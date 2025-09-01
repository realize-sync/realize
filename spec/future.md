# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## cache umask {#cacheumask}

It should be possible to specify a umask for cache entries, to
configure who can access the entries and whether write access is
allowed.

Write access doesn't really apply to the cache, which is read-only,
but is followed by OverlayFs, which might then allow local
modification of remote files.

## configure "auth" logs {#logauth}

setup configuration so that connection attempts and auth error/accept
can easily be singled out.

Currently connection attempts appear as, for example:

```
...DEBUG realize_network::network] 199.45.155.104:49506: connection rejected: received corrupt message of type InvalidContentType
```

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

## Trim history {#trimhistory}

Decide on rules for trimming history.

## It should be possible to read a large file without storing it {#largedl}

Currently, when accessing remote files, the data is always downloaded.
If the file is kept as long as it's open, even if it's too large to
fit in the working area. That's wrong.

## Support editing files through overlay fs {#overlaymod}

1. edit

Editing files through overlay fs looks like an already cached file is
added to the index with a new version.

This should be reported to other peers as a modification of the cached
version.

2. move directory

When a remote directory is moved, a xattr is stored locally. The move should be applied remotely.

From https://docs.kernel.org/filesystems/overlayfs.html:
""the directory will be copied up (but not the contents). Then the “trusted.overlay.redirect” extended attribute is set to the path of the original location from the root of the overlay. Finally the directory is moved to the new location."

3. move (rename) a remote file

What will happen? Proper support might require decoupling blobs from inodes.

4. hard link from a remote file to a local file

What will happen?


## Read and write marks from xattrs {#marksxattrs}

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
