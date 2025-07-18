# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Track path marks {#marks}

Arenas, files and directories can be marked *own*, *watch* or *keep*
as described in the section Consensus of [real.md](real.md) and the
section Blobstore in [unreal.md](unreal.md)

- Files marked *own* belong in the real. They should be moved into the
  arena root as regular file.

- Files marked *watch* or *keep* belong in the unreal. They should be
  left in the cache. They might be moved to the cache if they're
  stored as regular file, once another peer has taken ownership of
  them.

- Files marked *watch* are subject to the normal LRU rules in the
  cache. They may be refreshed when they change.

- Files marked *keep* are unconditionally kept in the cache and
  refreshed when they change.

- Unmarked files inherit their mark from the containing directory or Arena.

1. Define an enum type `Mark` and an arena-specific type `PathMarks`
   that tracks marks hierarchically by paths in the file
   `../crate/realize-storage/src/mark.rs`. For now, `PathMarks` has
   just a global (per-Arena mark) and returns that for every path it's
   given to `PathMarks::for_path(&realize_types::Path) -> Mark`

   Add the new types then run `cargo check -p realize-storage --lib`,
   fix any issues.

2. Add a `Mark` to `ArenaConfig` and add a function
   `PathMarks::from_config(&ArenaConfig)->Self` to create a
   `PathMarks` from config. The mark defaults to *watch* in
   ArenaConfig.

   Refactor, then run `cargo check -p realize-storage --lib` and
   `cargo test -p realize-storage`, fix any issues.

3. Extend `PathMarks` to keep paths, which can be file or directory,
   and the corresponding mark. Extend `for_path` to take these paths
   into account (return the file mark if it is set, return the
   containing directory mark if it is set, return the default mark
   otherwise).

   Marks can be set and unset (for the next step)

   Add unit tests for `for_path` that tests file, directory and arena
   marks.

   - Implement, then run `cargo check -p realize-storage --lib`, fix
   any issues.
   - Add unit tests, then run `cargo test -p realize-storage marks`,
   fix any issues.
   - Finally run `cargo test -p realize-storage`, fix any issues.

4. Extend `Watcher`, defined in
   [watcher.rs](../crate/realize-storage/src/real/watcher.rs) to track
   xattr `user.realize.mark` on files and directories and update
   `PathMarks` based on that.

   The `Watcher` should be given a `PathMarks` type to modify.
   Typically that type is empty with just the default (arena) mark
   set. It returns a reference to that `PathMarks` instances from
   `Watcher::path_marks() -> &PathMarks`

   During catchup, the Watcher should just set marks as it finds them
   in the `PathMarks` it's given without worrying about existing
   marks.

   When receiving a notification, the watcher should set, update and
   unset marks as xattrs change.

   - Implement, then run `cargo check -p realize-storage --lib`, fix
   any issues.
   - Add unit tests, then run `cargo test -p realize-storage marks`,
   fix any issues.
   - Finally run `cargo test -p realize-storage`, fix any issues.


## Design and implement decision maker {#decisionmaker}

Design a type that makes decisions and store them in Engine based on:
- `PathMarks`
- notifications from other peers
- notification from the index

TODO: fill that in


## Turn Blobstore into a LRU cache {#bloblru}

Add a single-level LRU cache for the Working cache of the blobstore,
described in the section BlobStore of [unreal.md](unreal.md)

Task list: TBD

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
