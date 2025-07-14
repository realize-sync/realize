# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Inode type {#inode}

A new Inode type is defined in
crate/realize-storage/src/unreal/types.rs that implements redb::Value
and redb::Key.

Let's use it everywhere it makes sense.

## Types and Structures to Update

### 1. **Data Structures in `unreal/types.rs`**
- `ReadDirEntry.inode: u64` → `ReadDirEntry.inode: Inode`
- `FileTableEntry.parent_inode: u64` → `FileTableEntry.parent_inode: Inode`
- `DirTableEntry::into_readdir_entry(self, inode: u64)` → `DirTableEntry::into_readdir_entry(self, inode: Inode)`

### 2. **Database Table Definitions**
- `ARENA_TABLE: TableDefinition<&str, u64>` → `ARENA_TABLE: TableDefinition<&str, Inode>`
- `INODE_RANGE_ALLOCATION_TABLE: TableDefinition<u64, u64>` → `INODE_RANGE_ALLOCATION_TABLE: TableDefinition<Inode, Inode>`
- `DIRECTORY_TABLE: TableDefinition<(u64, &str), Holder<DirTableEntry>>` → `DIRECTORY_TABLE: TableDefinition<(Inode, &str), Holder<DirTableEntry>>`
- `FILE_TABLE: TableDefinition<(u64, &str), Holder<FileTableEntry>>` → `FILE_TABLE: TableDefinition<(Inode, &str), Holder<FileTableEntry>>`
- `PENDING_CATCHUP_TABLE: TableDefinition<(&str, u64), u64>` → `PENDING_CATCHUP_TABLE: TableDefinition<(&str, Inode), Inode>`
- `CURRENT_INODE_RANGE_TABLE: TableDefinition<(), (u64, u64)>` → `CURRENT_INODE_RANGE_TABLE: TableDefinition<(), (Inode, Inode)>`
- `BLOB_TABLE: TableDefinition<u64, Holder<BlobTableEntry>>` → `BLOB_TABLE: TableDefinition<Inode, Holder<BlobTableEntry>>`
- `HISTORY_TABLE: TableDefinition<u64, Holder<HistoryTableEntry>>` → `HISTORY_TABLE: TableDefinition<Inode, Holder<HistoryTableEntry>>`

### 3. **Function Signatures in `unreal/cache.rs`**
- `lookup(parent_inode: u64, name: &str)` → `lookup(parent_inode: Inode, name: &str)`
- `lookup_path(arena: &Arena, path: &Path) -> Result<(u64, InodeAssignment)>` → `lookup_path(arena: &Arena, path: &Path) -> Result<(Inode, InodeAssignment)>`
- `file_metadata(inode: u64)` → `file_metadata(inode: Inode)`
- `dir_mtime(inode: u64)` → `dir_mtime(inode: Inode)`
- `file_availability(inode: u64)` → `file_availability(inode: Inode)`
- `readdir(inode: u64)` → `readdir(inode: Inode)`
- `open_file(inode: u64)` → `open_file(inode: Inode)`
- `arena_for_inode(txn: &ReadTransaction, inode: u64)` → `arena_for_inode(txn: &ReadTransaction, inode: Inode)`
- `alloc_inode_range(arena: &Arena) -> Result<(u64, u64)>` → `alloc_inode_range(arena: &Arena) -> Result<(Inode, Inode)>`

### 4. **Function Signatures in `unreal/arena_cache.rs`**
- `lookup_path(path: &Path) -> Result<(u64, InodeAssignment)>` → `lookup_path(path: &Path) -> Result<(Inode, InodeAssignment)>`
- `file_metadata(inode: u64)` → `file_metadata(inode: Inode)`
- `dir_mtime(inode: u64)` → `dir_mtime(inode: Inode)`
- `file_availability(inode: u64)` → `file_availability(inode: Inode)`
- `readdir(inode: u64)` → `readdir(inode: Inode)`
- `open_file(inode: u64)` → `open_file(inode: Inode)`

### 5. **Function Signatures in `realize-core`**
- `downloader.rs: reader(inode: u64)` → `reader(inode: Inode)`
- `nfs.rs: build_file_attr(inode: u64, metadata: &FileMetadata)` → `build_file_attr(inode: Inode, metadata: &FileMetadata)`
- `nfs.rs: build_dir_attr(inode: u64, mtime: &UnixTime)` → `build_dir_attr(inode: Inode, mtime: &UnixTime)`

## Transition Plan

### Phase 1: Update Data Structures (Low Risk)
1. **Update `unreal/types.rs`**:
   - Change `ReadDirEntry.inode` from `u64` to `Inode`
   - Change `FileTableEntry.parent_inode` from `u64` to `Inode`
   - Update `DirTableEntry::into_readdir_entry` parameter
   - Update all constructors and methods that use these fields

2. **Update Cap'n Proto serialization**:
   - Modify `ByteConvertible` implementations to handle `Inode` instead of `u64`
   - Update `from_bytes` and `to_bytes` methods

### Phase 2: Update Database Schema (Medium Risk)
1. **Update Table Definitions**:
   - Change all `TableDefinition` types that use `u64` for inodes to use `Inode`
   - This will require database migration or recreation

2. **Update Database Operations**:
   - Modify all database read/write operations to use `Inode`
   - Update range queries and comparisons

### Phase 3: Update Function Signatures (High Risk)
1. **Update Storage Layer**:
   - Change all function signatures in `unreal/cache.rs` and `unreal/arena_cache.rs`
   - Update all callers to pass `Inode` instead of `u64`

2. **Update Core Layer**:
   - Change function signatures in `realize-core`
   - Update all callers in the filesystem and RPC layers

### Phase 4: Update Tests and Documentation
1. **Update all tests** to use `Inode` instead of `u64`
2. **Update documentation** and examples
3. **Add migration tests** to ensure data integrity

## Benefits of This Transition

1. **Type Safety**: Prevents mixing inode numbers with other u64 values
2. **Database Safety**: Ensures inodes are properly serialized/deserialized
3. **API Clarity**: Makes it clear when a value represents an inode
4. **Future Extensibility**: Allows adding inode-specific methods and validation

## Risks and Considerations

1. **Breaking Changes**: This is a major breaking change that affects the entire codebase
2. **Database Migration**: Existing databases will need to be migrated or recreated
3. **Performance**: Minimal impact, but `Inode` is slightly larger than `u64` due to newtype overhead
4. **Compatibility**: External APIs that expect u64 inodes will need to be updated

Would you like me to start with Phase 1 (updating the data structures) or would you prefer a different approach?

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
    do_write_file_entry and do_rm_file_entry with Option<Peer> set to
    None or the key set to (inode, "")).

    Add a test to check that creates a new file with Fixture::add_file
    call file_open, close the returned file and then:
    - overwrite the file with a new version, like in
      replace_existing_file. Make sure the blob and file created by
      open_file() have been deleted
    - delete the file, like in unlink_removes_file. Make sure the
      blob and file created by open_file() have been deleted.

    Run "cargo check -p realize-storage", then "cargo test -p
    realize-storage" and fix any issues.

 5. Add function to ArneaUnrealCacheConfig for getting and updating
    the ByteRange of a blob given the blob_id returned by file_open.
    The function for updating the ByteRange is meant to only be called
    after updating the file content flushing and syncing. Add the same
    functions to UnrealCacheConfig that just delegate to
    ArenaUnrealCacheConfig.

    Do not add these functions to UnrealCacheAsync, as the type
    returned by UnrealCacheAsync is supposed to be what is going to
    update the ByteRange in the next step.

    Add a unit test that call the functions in UnrealCacheConfig and
    verify the ByteRange. Run the test and make sure it passes.

 6. Change the type returned by UnrealCacheAsync::file_open to Blob,
    which implements tokio::io::AsyncRead and tokio::io::AsyncSeek
    (look for "impl AsyncRead" in the codebase for examples of such
    implementation) by delegating to the equivalent function on the
    tokio::std::File, except in the case where a read starts outside
    the bounds of the blob's available ByteRanges (returned by
    open_file, kept as a field in the Blob). In such case, return a
    new error type BlobIncomplete wrapped in std::io::Error::other().

    Write tests for reading
    - within the ByteRanges:
    - starting just before the end of the ByteRanges (the beginning of
    the data to be read is in range, but the end isn't, so just
    the beginning is returned (short read))
    - starting after the end of the ByteRange (which throws BlobIncomplete)

    Run the tests and make sure they pass.

 7. TBD Add AsyncWrite to Blob.

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
