# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Use hash replacement chain in cache {#cachehash}

Currently, if a file is available in multiple peers, the cache returns
one of the most recent ones. This logic predates hashes.

With hashes, it should be possible for a pair of hashes A and B,
looking at the peers chain of hashs reported by hash and replace,
whether hash A replaces hash B.

If we have, for example:

 - peer1: add(hash A), replace(hash B, hash A)
 - peer2: add(hash A)

then we know that we should return hash B.

This information might be taken into account at write time.

It's only in the case of catchups that this information might not be
available. Possibly it should be made available in a best-effort basis
(in case the history table was trimmed), by going through the list of
replace and reporting them during catchup.

TBD detailed task list

## Remove path from FileContent {#cachepath}

`FileContent`, defined in
[unreal.rs](crate/realize-lib/src/storage/unreal.rs) stores arena and
path as strings. This is unnecessary, since the position of the file
in the hierarchy specifies that.

What is needed is a way of going from a file inode back up to the
root. It's already possible to go back up to a parent directory, since
`FileTableEntry` contains `parent_inode`. `ReadDirEntry` should be
extended with a `parent_inode` as well.

The field `arena` in `FileContent` is also used by `mark_peer_files`
in [sync.rs](crate/realize-lib/src/storage/unreal/sync.rs) to find out
which files belong to a specific arena. Let's replace `arena` by
`arena_root`, an u64 from `arena_map` from `UnrealCacheBlocking`, which
should be transformed into a bijective map.

Such a `FileTableEntry` would unfortunately not be usable by the
downloader anymore, so `file_availability` in `UnrealCacheBlocking`
and `UnrealCacheAsync` must be updated to return the information the
downloader needs: peer, arena, path, hash.

TBD: Detailed task list

## Allow : in model:Path {#colon}

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
