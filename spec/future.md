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

## Change Database ownership {#owndb}

The goal of this change is to allow different classes to share the
same database. This allows putting the arena cache and real index in
the same database.

Task list:

1. Go through all redb table definition to add the current subsystem
   to their name. Databases defined in
   [index.rs](../crate/realize-storage/src/real/index.rs) should have
   an "index." prefix added to their names. Databases defined in
   [arena_cache.rs](../crate/realize-storage/src/unreal/arena_cache.rs)
   should have a "acache." prefix. Databases defined in
   [cache.rs](../crate/realize-storage/src/unreal/cache.rs) should
   have a "cache." prefix.

   Change the database names then run `cargo check -p realize-storage
   --tests`, fix any issues.

2. Make `redb::Database` an Arc<Database> everywhere and move database
   creation out from `ArenaCache` defined in
   [arena_cache.rs](../crate/realize-storage/src/unreal/arena_cache.rs)
   or `RealIndexBlocking` defined in
   [index.rs](../crate/realize-storage/src/real/index.rs). Their
   primary constructor receive an Arc<Database> instead of a file. Let
   their caller create a database from file or memory.

   Keep the file/database relationship the same otherwise.

   Refactor existing code then run `cargo check -p realize-storage
   --lib`, then `cargo check -p realize-storage --tests` then `cargo
   test -p realize-storage` to make sure everything still works. Fix
   any issues.

3. Update the storage
   [config.rs](../crate/realize-storage/src/config.rs) to take only
   one database per arena. Then, when building `Storage` from config,
   in the [storage module](../crate/realize-storage/src/lib.rs), put
   both the index and the area cache into that database.

   Do the same when creating a `Storage` for testing in
   [testing.rs](../crate/realize-storage/src/testing.rs)

   Refactor existing code then run `cargo check -p realize-storage
   --lib`, then `cargo check -p realize-storage --tests` then `cargo
   test -p realize-storage` to make sure everything still works. Fix
   any issues.

3. Extract a `Blobstore` type from `ArenaCache` that share the same
   DB and might also share the same transaction. Details TBD

## Add an Engine {#engine}

An engine is a new type, defined in
`crate/realize-storage/src/engine.rs` that store `Decision`s in a
redb::Database it's given.

Implement `engine.rs` database-handling just the way it's defined in
[index.rs](../crate/realize-storage/src/real/index.rs), that is:

- Define two types, `EngineAsync` and `EngineBlocking`.

    `EngineAsync` just keeps an `EngineBlocking` that it calls using spawn_blocking.
    `EngineBlocking` does the real work. It is given an `Arc<Database>` and stores its data here.

- Define a redb::Table with a path as key (stored as a &str) and a
  `Holder<DecisionTableEntry>` as value. `DecisionTableEntry` is a
  rust type defined in `engine.rs` that can be serialized to and
  deserialized from a capnp message, defined in
  `crate/realize-storage/capnp/engine.capnp`

  `DecisionTableEntry` just contains a single enum for now, a
  `Decision` which has the values:
     - `Realize` -- move from cache to arena root (as a regular file)
     - `Unrealize` -- move from arena root to cache
     - `UpdateCache` -- update the file from another peer and store it in the cache

- Define the following methods on `EngineBlocking` and `EngineAsync`:
  `set(path, decision)`, `clear(path)`, `get(path)`. Adds adds or
  updates an entry in the database. Clear removes any entry for the
  path from the database (if one existed), get returns the `Decision`
  for the path, if any exist.

Task List:

1. Create the file `engine.rs`, with skeletons for the two types
   `EngineAsync` and `EngineBlocking` and the methods that link them
   together (`EngineBlocking::into_async()`
   `EngineAsync::new(EngineBlocking)` `EngineAsync::blocking()` like
   in `index.rs`)

   Write the code, then run `cargo check -p realize-storage --lib` to
   make sure it compiles.

2. Define the types `Decision` and `DecisionTableEntry` and have it
   implement `ByteConvertible` so it can be put into a `Holder`

   This requires defining the corresponding capnp enum and type in
   `crate/realize-storage/capnp/engine.capnp`. Set the parent module
   to "engine". Make sure to update
   [build.rs](../crate/realize-storage/build.rs) and define the module
   engine_capnp in `engine.rs` just like it's done in
   [real.rs](../crate/realize-storage/src/real.rs) for `real_capnp`.

   Write the code, add unit tests for serialization/deserialization,
   then run `cargo test -p realize-storage engine`

3. Define the new DECISION_TABLE (name: "engine.decision", key: &str,
   value: Holder<DecisionTableEntry>) and implement the methods `set`,
   `clear` and `get` in `EngineBlocking` as well as the corresponding
   methods in `EngineAsync` that delegate to them.

   Write the code, add unit tests then run `cargo test -p
   realize-storage engine`. You can create an in-memory database for
   the tests using
   `redb::Builder::new().create_with_backend(redb::backends::InMemoryBackend::new())?`

   When writing the tests, make sure to add a fixture and use it
   throughout to avoid duplicating work in all test:

```
   struct Fixture { engine: EngineBlocking }
   impl Fixture {
     fn setup() -> Self {
        let engine = ... // create db and type

        Fixture { engine }
     }
   }

   #[test]
   fn engine_does_something() -> anyhow::Result<()> {
      let fixture = Fixture::setup()?;

      fixture.engine.do_something()?;

      Ok(())
   }
```

(Next steps: Design and implement the code that creates and updates
the decisions. That's outside the scope of this section.)

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
