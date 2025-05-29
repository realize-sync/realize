# Realize Filesystem

## Overview

Implement a FUSE filesystem with 2 layers:

 - the Real, that is, the files that are available locally and
   complete, stored on fs as file

 - the Unreal, that is, the files that are not completely available
   locally, kept in a key-value store and served directly from it.

Each peer keeps track of local Real and Unreal data. Some peers might
want to keep a copy of all files in the Real, other might want to just
cache the files that are needed in the Unreal and realize them on
demand only.

Peers keep a local history of local modifications to both the Real and
the Unreal (create, delete, rename, edit, overwrite) to be sent to
other peers.

Extra operations are available on the files and directories of the
filesystem to mark them as :

  - Real: the file or directory content must be kept locally

  - Unreal: the file or directory content must be kept in the cache.
    New, edited or overwritten files are still kept in the real for a
    while but they'll move back to the unreal after Consensus.

New files share the setting of the containing directory. That setting
is only kept in the Real.

### Concepts

  - *The Real* files available locally and their directories.

  - *The Unreal* Remote paths, copied locally and blobs not
    available or not completely available locally

  - *An Area* a set of path and their assignment to blobs. Note that
    directories only exist as path elements in the Unreal.

  - *Realize* put data from the Unreal into a local file

  - *Unrealize* move a local file to cache and let cache eviction
    strategy deal with it

  - *The Household* set of peers that sync some areas of data. Some peers
    might only be interested in a subset of the Household's areas.

  - *A peer* member of the Houshold, known by its public key, with which
    communication is possible.

  - *A listening peer* a peer with a known, stable address that can be
    connected to.

  - *A deaf peer* a peer that's not listening to a known address. It
    cannot be connected to. (Deaf peers can export services to peers
    they connect to.)

  - *A Blob* a piece of data, identified by its hash. A blob can be
    based on (edit) another blob, using byte-based patches (Blob B =
    Blob A + patch)

  - *A conflict* when two peers have different blobs assigned to the
    same path.

  - *Delete* unlink a blob from its path

  - *Rename* unlink a blob from path A, link it to path B

  - *Change* unlink the old blob from a path, link a new blob to that
    path. Note that the new blob might be based on the old blob.

  - *Edit* when a blob is based on another blob, it is an edit of the
    old blob.

  - *Create* link a blob to a path

## Details

### The Real

The Real contains locally-available files, split by Area.

### The Unreal

The Unreal contains remotely-available files, with, for each peer:

 - the peer id

- the address to connect to (unless the peer is the one to connect)

- a list Areas, identified by their name

- for each Area, list of remotely-available files (path) and their
  size (+more information? what about duplicates?)

While connected, the local server gets notified whenever files are
added, removed or changed (with a delay to make sure file is stable).
This way, the file list is always available locally.

While disconnected, the local server can still serve the last known
list of files, and the cached files.

Making change a file that's part of the Unreal moves it into the Real,
like when deleting, renaming, overwriting or editing.

File data that's part of the Unreal works as a cache: data that's
worked on often is kept in cache, older data is evicted as needed to
make space. The Real and the Unreal of an Area might compete for space
(cache data might need to be removed to allow a Real file to be
added.)

Different Areas might also compete for space, depending on the local
settings.

### Local History

For each Area, a history of file addition, edit, overwrite, move and
deletion that are yet to be sent to peers. History sections are
removed once it's been sent to all known peers.

### Real → Unreal

When a file is added to the Real, it is stored locally until Consensus
time, that is, the time where two peers sync their local changes.

Consensus time can be:

 - on a schedule

 - immediately, after a change for some operations (delete, rename)

 - after a rest period, for some operations (file creation, overwrite,
   edit)

After consensus has been reached, a file that was Real can become
Unreal, depending on the local settings and cache size.

TODO: resolve conflicts during consensus.

### Unreal → Real

When a file is marked as Real, it is fully downloaded into the cache.
Once fully available, it is realized, that is, turned into a file.

When a file is needed, it *might* be fully downloaded and turned into
a file, but only for a time (how long?)

## Milestones

1. Move files from one peer to the other (current as of 2025-05-29)

2. A passthrough filesystem that makes the real layer of areas
   available, tracks history for peers. On Consensus time (started
   manually), path (re)assignments and blobs from history.

3. Add unreal layer to filesystem. Track remove file list locally.
   Unreal files can be read, but that requires network access. History
   is sent to connected peers as soon as possible. Consensus remains a
   manual operation; conflicts are not resolved and blobs assigned to
   paths just diverge, as conflicting changes stay local.

4. Add cache to unreal layer.

5. …


## Choices

### Key-value store: redb

1. [redb](https://github.com/cberner/redb/tree/master)

   - ➕ Maintained
   - ➕ Simple, good performance
   - ➕ Crash-safe
   - ➕ Designed for a read-heavy workload
   - ➕ Type-safe

2. [sled](https://github.com/spacejam/sled)

   - ➕ Maintained
   - ➖ Being rewritten
   - ➕ Powerful merge operators
   - ➖ Regular fsync (every 500ms) for crash safetly
   - ➖ Slightly worse performance than redb according to
     [benchmarks](https://github.com/cberner/redb/tree/master?tab=readme-ov-file#benchmarks)

### FUSE library: fuse-backend-rs

1. [fuse-backend-rs](https://github.com/cloud-hypervisor/fuse-backend-rs)
   - ➕ Maintained
   - ➕ Real usage
   - ➕ Great passthrough support
   - ➕ Sync and async
   - ➖ Uses raw C types everywhere (for passthrough)

2. [fuse-rs](https://github.com/zargony/fuse-rs)
   - ➕ Good rust types everywhere
   - ➕ Easy to use
   - ➖ Is it still maintained? Last commit was 5 years ago!

### FUSE vs NFSv3

FUSE is difficult to make work on MacOS, impossible on Windows. A
better cross-platform solution would be to use the approach of
[nfsserve](https://github.com/xetdata/nfsserve) and build a NFSv3
server, then bind it using existing OS tools.

FUSE

 - ➕ Powerful
 - ➖ Linux-only, some support on MacOs, no support on Window

NFSv3

 - ➖ Limited: no xattr, no file watch
 - ➖ Some apps that are storage-based don't work well on NFS, but
   have no problems on a FUSE passthrough

## Security

Peers of a household know all other peers by their public key.
Connections are made using TLS 1.3 with raw keys (ED25519). Both
client and server only accept connections made with a known peer key.

## Testing

