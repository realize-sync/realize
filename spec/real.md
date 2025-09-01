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

#### Index Database Tables

The tables elow keep an index of the files, their content (hash) and
history. Local modifications on top of remote files still TDB.

** File Table **

Stores file and hash, together with metadata.

A hash is only valid as long as the mtime and size match the current
file.

When adding a new entry, the current value is used to populate the
old_hash field of the corresponding notification. `notification_index`
is the key of the corresponding notification in the history table. It
might not exist anymore.

The key doesn't include the arena; each arena has its own separate
index database.

* Key: `&str`  `model::Path`
* Value: `FileTableEntry: {Hash, mtime: UnixTime, size: u64, notification_index: u32}`

** History Table **

Keeps changes, in order, so they can be served to peers.

* Key: `u64` index (monotonically increasing)
* Value: `HistoryTableEntry: {Notification}`

** Peer History Table **

Keeps track of the index of most recent notification successfully set
to each peer and the (local) time at which this was sent.

This is used to delete old entries from the history table: entries in
the history table can be removed once read by all peers or a peer has
been disconnected for so long that it'll have to start from scratch,
without history.

* Key: `&str` `Peer`
* Value: `PeerTableEntry: (index: u64, SystemTime)`

### History

The history tracks, within an arena (defined in [the overall
design](./design.md)), the following modifications:

`Notification`:

- `Add(arena, path, mtime, size, hash)`: set file content,
  versioned by its hash.

- `Replace(arena, path, mtime, size, hash, old_hash)`: replace
  file content hash old_hash with new file content with the given
  hash and metadata.

- `Available(arena, path, mtime, size, hash)`: remote file was
  downloaded and made available locally

- `Remove(arena, path, old_hash)`: Remove file content identified
  by old_hash.

- `Drop(arena, path, hash)`: file was removed locally, but is still
  available remotely

> [!NOTE] Phase 1 also includes CatchupStart(arena), Catchup(arena,
> path, mtime), Ready(arena) This is gone in phase 2, described here.
> For a description of catchup, see the description of
> mark_peer_files/delete_marked_files in [The Unreal](./unreal.md)

The difference between `Remove` and `Drop` is that the former should
be replicated to other peers while the latter is just informational.
`Add`/`Replace` and `Available` pair have a similar relationship.

The [Unreal cache](./unreal.md) doesn't distinguish `Add`/`Replace`
from `Available` nor `Remove` from `Drop`.

This is mapped from lower-level inotify notifications. For example,
Inotify reports just a directory move, which results in many files
being reported as deleted and moved in the history.

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

### Update from Peers

Peers listen to notifications from remote peer's [History] and apply
`Add`, `Replace` and `Remove` modifications locally.

- `Remove` is ignored if a file exists whose content doesn't match
  old_hash.

- `Replace` is ignored if a file exists whose content doesn't match
  old_hash. If no file exists, `Replace` is treated as `Add`.

- if local file with matching hash has been reported as `Remove`,
  delete it

- if local file with matching hash has been reported as being
  modified by `Replace`, download the new version

- if no local file exists reported by `Replace` or `Add` and it is in
  an *own* directory, download it

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

How that happens is detailed in [Consensus](consensus.md)

## Future
