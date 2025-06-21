# The Real - Merging local data and local modifications with the Unreal

## Overview

Use [overlayfs](https://docs.kernel.org/filesystems/overlayfs.html) to
expose a merged view of the local files and [The Unreal](unreal.md)
filesystem exporting remote files. 

When a remotely available file is modified, the changes are applied
locally and later on sent to other peers.

This match OverlayFS behavior, as changes are always applied to the
upper layer, the local files, even when they apply to the lower layer,
the remote files. This means that local modifications are stored
locally by OverlayFS, either as plain files, or as specially marked
files (with metacopy=on,redirect_dir=on).

These local modifications should then be reported to other peers,
which might choose to apply them and report them back as applied, so
local changes can eventually be dropped.

Since this design relies on OverlayFS, it is a Linux-only solution.
The proposed cross-platform solution is to make The Unreal read-write
and is outside the scope of this document.

## Stages

Design and implementation happens in stages, incrementally:

1. Report local changes as `link`, `unlink` notifications with a
   catchup phase to deals with unknown changes happening between
   disconnections, described in [The Unreal](./unreal.md) and
   implemented based on
   [inotify](https://man7.org/linux/man-pages/man7/inotify.7.html).
   
2. Track changes and file content hash in an index table. Upon
   restart, catchup with the latest changes by comparing index with
   directory content, is a requirement for phase 3 and 4. [Index]
   
3. Report local change to remote data to peers and update local data
   on interested peers (sync) using such reports. Remove local data
   after synced for files we don't want to keep locally. [History]

4. Switch `RealStoreService` to using blobs, identified with hashes to
   download data. This allows [The Unreal](./unreal.md) to store and
   serve file data. [Future](./unreal.md#Future)

## Details

### Overlay

A storage directory must be made available to store:
 A. the data for The Unreal database and files
 B. the data for the Real files
 C. the difference between the two
 D. a work directory for temporary files

D is an OverlayFS requirement

OverlayFS can store B and C, C is mostly stored in xattrs as described
on [overlayfs](https://docs.kernel.org/filesystems/overlayfs.html). It
is important to turn on the options metacopy and redirect_dir, so
OverlayFS stores dir changes as redirects and keeps metadata changes
local, without having to copy the whole file.

As file metadata changes include xattrs, xattrs can be used to let
users specify whether a file (remote or local) should be kept locally
or not.

### Index

To be able to build [History], we need to track changes to the
directory content. 

While the process is up and running, changes are tracked using
inotify. While the process is down, change aren't tracked and need to
be caught up at startup. This is where the index comes in.

The index tracks: 
- the files it knows about and their content (mtime + hash)
- the local modifications from OveralyFS (in xattrs and special files)

This information is used to build a history of changes at startup, by
comparing the content of the index with the content of the directory.

Exact format and table layout TBD

### History

The history tracks, within an arena (defined in [the overall
design](./design.md)), the following modifications:

`Notification`:

- `Link(arena, path, mtime, blob)`: file content set to blob, identified by its hash

- `Available(arena, path, mtime, blob)`: remote file was downloaded and made available locally

- `Unlink(arena, path, mtime)`: file has no content anymore

- `Drop(arena, path, mtime)`: file was removed locally, but is still available remotely

> [!NOTE] Phase 1 also includes CatchupStart(arena), Catchup(arena,
> path, mtime), Ready(arena) This is gone in phase 2, described here.
> For a description of catchup, see the description of 
> mark_peer_files/delete_marked_files in [The Unreal](./unreal.md)

The difference between `Unlink` and `Drop` is that the former should
be replicated to other peers while the latter is just informational.
The `Link` and `Available` pair have a similar relationship.

The [Unreal cache](./unreal.md) doesn't distinguish `Link` from
`Available` nor `Unlink` from `Drop`.

This is mapped from lower-level inotify notifications. For example,
Inotify reports just a directory move, which results in many files
being reported as deleted and moved in the history.

Exact mapping TBD.

These notifications are sent to peers through the `HistoryService`

#### HistoryService

The history service allows peer A connecting to peer B to be notified
of changes on peer B local store. 

Peer B calls the following RPC on peer A:

```rust
    /// Arenas the connecting peer is interested in. 
    async fn arenas() -> Vec<Arena>;
    
    /// Local modifications reported by the connected peer.
    async fn notify(batch: Vec<Notification>);
```

`Notification` is described in the previous section.

## Future

TBD
