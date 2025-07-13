# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## An unreal cache per arena 1: inode allocation {#arenacache1}

The separation between the global cache and per-arena blobstore in the
design, in [unreal.md](unreal.md) causes unnecessary issues. Things
would be simpler if we had a per-arena cache.

The single thing that really needs to be global in the cache is inode
allocation and lookup, as everything ends up in the same filesystem,
so to split out the cache, we're going to start by splitting out inode
allocation and lookup in the database, then split out the database.

Lookup is the difficult part: given an inode, how to we know which
arena to look up information in? To answer this question quickly in
the global cache, let's allocate large range of inodes to arenas:
whenever an arena cache runs out of inode, it asks the global cache
for a new range. The global cache looks into its range allocation
table for an unassigned range of u64, assign it to the arena and
returns it.

Inode allocation in
[cache.rs](../crate/realize-storage/src/unreal/cache.rs) is currently
very simple: there's a MAX_INODE_TABLE that stores the max inode
that's updated every time a new inode is added. This will be split
into a per-arena CURRENT_INODE_RANGE_TABLE and a global
INODE_RANGE_ALLOCATION_TABLE.

**INODE_RANGE_ALLOCATION_TABLE**

Key: u64 (inode)
Value: &str (arena) "" to allocate global inodes

The allocates increasing range, starting at 0, so only the
(the last value within the range) needs to be stored.

To figure out what range a given number N is in, lookup the range
[N..]; the first element that's returned is the end of the current
range, to which N belongs.

To allocate a range, lookup the last element of the table (0 if
absent), let's call it A, add the number of inodes to allocate (10000
for example), let's call it B, then insert (B, arena). Return the
range [last_element+1; B+1) (The +1 are because that range is start
inclusive, and end exclusive as per usual)

**CURRENT_INODE_RANGE**

Key: &str (arena, before the cache is really split)
Value: (u64, u64) (current, end)

To allocate a new inode for an arena:

- query the range.
- If missing, ask for a new allocation.
- If current < end, use that allocation: add 1 to current, write that number and return it.
- Otherwise, ask for a new allocation;
- let's say we got the range [A, B) as new allocation. Write current=A, end=B and return A

Add 1 to current.
If current < end, write that number as new current and return it.
Otherwise, ask for a new allocation.

Task list:

1. Add new table definitions to
   crate/realize-storage/src/unreal/cache.rs. Make sure everything
   compiles with "cargo check -p realize-storage"
2. Write code for allocating a range to an arena and add unit tests
   for it. Make sure the unit tests pass with with "cargo check -p
   realize-storage ::cache"
3. Rewrite UnrealCacheBlocking::alloc_inode to allocate a new inode
   for an arena. This requires knowing what arena we're in when adding
   a new entry, from the update() method. This requires some
   refactorings.

   Note that the inodes allocated in add_arena are special: the inode
   allocated to the arena root must be allocated in the arena range,
   but the inodes allocated to a directory before the arena root must
   not be allocated in the arena range (So for an arena called
   "documents/letter" the inode for document is in the non-arena range
   but the inode for letter is in the arena range")

   Write units tests for it and make sure they pass with "cargo check
   -p realize-storage ::cache"

4. Write an inode-range lookup function lookup_arena(inode: u64) that
   is given an inode and returns the arena the inode belongs to. Write
   unit tests for it and make sure they pass with "cargo check -p
   realize-storage ::cache"

Next step is to actually split the cache. That's part of the next section.

Status: DONE

Follow-up: update the design doc after this change.

## An unreal cache per arena 2: split the cache {#arenacache2}

The goal of this change is to split UnrealCacheBlocking, defined in
[cache.rs](../crate/realize-storage/src/unreal/cache.rs), into
UnrealCacheBlocking (global) and ArenaUnrealCacheBlocking (per-arena).
The interface of UnrealCacheBlocking doesn't change, but it delegates.
most of the work to an arena-specific ArenaUnrealCacheBlocking
instance.

This continues the work done in the previous section #arenacache1, which
introduced a way of allocating and dispatching inodes per arena (lookup_arena(inode))

ArenaUnrealCacheBlocking interface mostly mirrors UnrealCacheBlocking,
without add_arena and with a new root() function to return the arena
root.

UnrealCacheBlocking keeps a hash map of ArenaUnrealCacheBlocking
instances.

Public methods of UnrealCacheBlocking either take an Arena (such as
update) or just an inode (such as lookup) and, in most case, just delegate to the relevant ArenaUnrealCachBlocking.


For example:
```
fn do_something(arena: &Arena, ...) -> ... {
...
}
```

Becomes:

```
fn do_something(arena: &Arena, ...) -> ... {
  cache_for_arena(arena)?.do_something(...)
}
```

The functions like the one above that take an Arena are easy to just
dispatch to ArenaUnrealCacheBlocking. The functions that take an inode
require calling lookup_arena(u64).


```
fn do_something(inode: u64, ...) -> ... {
...
}
```

Becomes:

```
fn do_something(inode: u64, ...) -> ... {
  cache_for_arena(lookup_arena(u64)?)?.do_something(...)
}
```

Now, to create an UnrealCacheBlocking::new, we need to pass it a
global database and a database for each arena. The same goes with
UnrealCacheBlocking::open.

UnrealCacheBlocking::add_arena should be integrated into
UnrealCachBlocking::new, since we know which arenas we'll support at
the beginning.

Status: DONE

Some follow-up cleanups might be useful:
 - remove unnecessary arenas from the table and table entries (see TODOs in cache.rs)
 - update the design doc after this change.

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


## Blobstore {#blobstore}

Implement an Arena-specific blob store as described in the section
Blobstore of [unreal.md](spec/unreal.md). Start with a blob store with
only one area/LRU queue; the Working area.

Task List:
 - define how the unreal cache and downloader interface with the
   blobstore and write skeleton with just that interface in
   crate/realize-storage/src/unreal/blobstore.rs with everything
   implemented as no-op, make sure it compiles
 - write capnp in crate/realize-storage/capnp/unreal/blobstore.capnp,
   add it to crate/realize-storage/capnp/unreal.rs, ake sure it compiles
 - implement a no-eviction version, add unit tests
 - write integration tests
 - add support for tracking cache size and doing eviction

## Remove path from FileContent {#cachepath}

`FileContent`, defined in
[unreal.rs](crate/realize-storage/src/unreal.rs) stores arena and path
as strings. This is unnecessary, since the position of the file in the
hierarchy specifies that.

What is needed is a way of going from a file inode back up to the
root. It's already possible to go back up to a parent directory, since
`FileTableEntry` contains `parent_inode`. `ReadDirEntry` should be
extended with a `parent_inode` as well.

The field `arena` in `FileContent` is also used by `mark_peer_files`
in [sync.rs](crate/realize-storage/src/unreal/sync.rs) to find out
which files belong to a specific arena. Let's replace `arena` by
`arena_root`, an u64 from `arena_map` from `UnrealCacheBlocking`,
which should be transformed into a bijective map.

Such a `FileTableEntry` would unfortunately not be usable by the
downloader anymore, so `file_availability` in `UnrealCacheBlocking`
and `UnrealCacheAsync` must be updated to return the information the
downloader needs: peer, arena, path, hash.

TBD: Detailed task list

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
