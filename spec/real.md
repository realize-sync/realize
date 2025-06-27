# The Real - Merging local data and local modifications with the Unreal

## Overview

Use [OverlayFS](https://docs.kernel.org/filesystems/overlayfs.html) to
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

1. In this phase, consensus consists of copying entire arenas from one
   peer to another using a command line tool, described in
   [movedirs](./movedirs.md)

2. Report local changes as `link`, `unlink` notifications with a
   catchup phase to deals with unknown changes happening between
   disconnections, described in [The Unreal](./unreal.md) and
   implemented based on
   [inotify](https://man7.org/linux/man-pages/man7/inotify.7.html).
   
3. Track changes and file content hash, used as both integrity check
   and version, in an index table. Upon restart, catchup with the
   latest changes by comparing index with directory content. This is a
   requirement for the next two phases. [Index]
   
4. Report local change to remote data to peers and update local data
   on interested peers (consensus) using such reports. Remove local
   data after synced for files we don't want to keep locally.
   [History] and [Consensus]

5. Use hashes to download the right version of a file or make sure
   file content is consistent. This allows [The Unreal](./unreal.md)
   to store and serve file data. [Future](./unreal.md#Future)

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

- `Link(arena, path, mtime, size, hash, old_hash)`: set file content,
  versioned by its hash. old_hash optionally identifies previous
  content for the path.

- `Available(arena, path, mtime, size, hash)`: remote file was
  downloaded and made available locally

- `Unlink(arena, path, old_hash)`: file has no content anymore.
  old_hash identifies the version that was removed

- `Drop(arena, path, hash)`: file was removed locally, but is still
  available remotely

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

These notifications are sent to peers through the `Store` interface.

#### Store interface

This RPC interface, define in `store.capnp` allows peer A connecting
to peer B to be notified of changes on peer B local store.

Peer B calls the following RPC on peer A:

```
interface Store {
    /// Arenas the connecting peer is interested in. 
    arenas @0 () -> (arenas: List(Text));
    
    /// Local modifications reported by the connected peer.
    subscribe @1 (arenas: List(Text), sub: Subscriber);
}

interface Subscriber {
    notify @0 (n: List(Notification));
}
```

`Notification` is described in the previous section.

### Consensus

Peers listen to notifications from remote peer's [History] and apply
`Link` and `Unlink` modifications locally.

`Unlink` and `Link` are ignored if old_hash doesn't match the current version, unless history is known to have been lost (catching up)

- if local file with matching hash has been reported as `Unlink`,
  delete it
  
- if local file with matching hash has been reported as being
  modified by `Link`, download the new version 
  
- if no local file exists reported by `Link` and it is in an 
  *own* directory, download it

- if local file marked *watch* has been reported as `Available` with
  the same hash, delete it

This leaves local file with a different hash than the hash reported by
a notification. Such conflict should be added to a table of conflicts
requiring manual intervention.

For that to work, files, directories and arenas roots can be marked
*watch* or *own*.

- if a directory or arena root is marked *own*, remote files should
  be replicated locally

- if a directory or arena root is marked *watch*, files in it are
  recursively marked *watch*

- if a directory is not marked either, it inherits from its parent,
  all the way up to the area root

- if a file is marked *own* it is kept locally. If there is no local
  copy yet, it should be downloaded.

- if a file is marked *watch* it is not kept locally. If there is a
  local copy, it should be deleted once another peer has it

- if a file is not marked either, it inherits from its parent directory

Marking of arenas is done in the configuration file and defaults here
to *watch*.

Marking of files and directories is done by setting an xattr on a
directory or a file. Exact xattr TDB.

The xattr might be replicated in the index, for convenience/speed, but
doesn't need to. This design relies on being able to apply xattrs on
files that are not available locally, through OverlayFS metacopy.

When the rule above call for downloading a file from a remote peer,
the desired action is kept into a queue to be executed later:

- When triggered from a command (as in phase 0, but limited to owned files)
- On a timer; it should be possible to configure times within which
  such download can happen, globally or per arena.
  
Downloading a file from a remote peer can be subject to rate-limits,
configurable globally or per arena.

## Future

TBD
