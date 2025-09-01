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

## Local modification of remote files

With [OverlayFS](https://docs.kernel.org/filesystems/overlayfs.html)
local modifications can be made to the remote cache filesystem. These
modifications are stored as file in the local filesystem and can be
sent to remote peers.

TODO: for each entry, when are the placeholders/whiteout deleted?

### Edit a file

When the [Watcher](../crate/realize-storage/src/arena/watcher.rs)
detects a new file in the index, it should check it in the cache. If
the file is in the cache, it should report the cached version as
original version.

It should do it regardless of whether OverlayFS sets the xattr
`user|trusted.overlay.origin` as it is possible to access the
directory directly.

When the index sees an Add, Replace or Delete that matches the file
and hash, it removes the special file and the corresponding entry.

*** OverlayFS ***

When a remote file is edited, OverlayFS stores the modified file
content in the local filesystem with the xattr
`user|trusted.overlay.origin` set.


Example:

remote:
```
echo "hello" > editme
```

local:
```
vi editme (write ", world" and save)
```

Result:
```
# file: editme content: hello, world
trusted.overlay.origin=0sAPshAIEAAAAAAAAAAAAAAAAAAAAAAAAAAH8EAAAAAAAA
```

### Delete a file

Detect 0-size files with xattr `user|trusted.overlay.whiteout` or
character devices with 0/0 device-number and treat them as whiteout.

Placing a whiteout on the file deletes the file. This is treated and
reported as file deletion except that the index entry contains
type=whiteout (instead of type=file).

When a whiteout is detected, check the cache. If there is a
corresponding file, store the whiteout with old_hash=cache hash and
report the file with that hash deleted to remote peers.

Catchup must wait until it has checked all possible redirection
destination before reporting such a file as deleted.

When the index sees an Add, Replace or Delete that matches the file
and hash, it removes the special file and the corresponding entry.

*** OverlayFS behavior ***

OverlayFS writes whiteout when a remote file or directory is deleted.

"A whiteout is created as a character device with 0/0 device number or
as a zero-size regular file with the xattr “trusted.overlay.whiteout”."

### Hard link

If a 0-byte file with the xattr `trusted.overlay.redirect` is detected
that points to a file in the cache different from the current one,
store it in the index with type=redirect and hash=cache.hash and
report it to history as:
 - Redirect from=<orig path> to=<dest path> hash=cache.hash

This is a new notification type.

Indexes that own <dest path> should replicate that hard link locally
and mark it owned as well, then report it normally. It'll then be
available in other caches.

Possible extension: Caches might detect this notification and store it
as a redirect waiting for the owner to report it as added.

When the index sees an Add, Replace or Delete that matches the file
and hash, it removes the special file and the corresponding entry.

*** OverlayFS behavior **

remote peer:
```
echo test >work/hardlinkme
```

local peer:
```
ln work/hardlinkme work/hardlinked
```

Result

```
# file: hardlinked 0-byte file
trusted.overlay.metacopy=""
trusted.overlay.origin=0sAPshAIEAAAAAAAAAAAAAAAAAAAAAAAAAAIAEAAAAAAAA
trusted.overlay.redirect="/work/hardlinkme"

# file: hardlinkme original file, content: "test"
trusted.overlay.metacopy=""
trusted.overlay.origin=0sAPshAIEAAAAAAAAAAAAAAAAAAAAAAAAAAIAEAAAAAAAA
trusted.overlay.redirect="/work/hardlinkme"
```

### Move/Rename a file

This is a combination of delete and hard link. The destination appears
as a hard line and the source has a whiteout.

It could be treated history events Delete(hash) + Redirect(from,hash),
with a twist, since it's important for Redirect to be handled before
Delete. To avoid ordering issues, the Watcher should generate a single
event Move(from,hash) based on inotify reporting this as a move.
During catchup, it's important to not report a deletion before having
looked for redirection.

This is a new notification type.

Indexes that own <dest path> should replicate the move locally, mark
the result owned as well, then report Add first, then Delete. It'll
then be available in other caches.

This is stored in the index in the source and destination file as a
type=MoveFrom and type=MoveTo.

Catchup must wait until it has checked all possible redirection
destination before reporting a file as deleted, as it's difficult to
tell deletion from MoveFrom.

Possible extension: Caches might detect this new notification and
store it as a redirect waiting for the owner to report it as added and
deleted.

When the index sees an Add, Replace or Delete that matches the file
and hash, it removes the special file and the corresponding entry for
both MoveFrom and MoveTo.

*** OverlayFS behavior **

remote peer:
```
echo test >moveme
```

local peer:
```
mv moveme moved
```

Result

# file: moveme char dev 0/0

# file: moved
trusted.overlay.metacopy=""
trusted.overlay.origin=0sAPshAIEAAAAAAAAAAAAAAAAAAAAAAAAAAIEEAAAAAAAA
trusted.overlay.redirect="moveme"

### Delete a directory

Detect 0-size files with xattr `user|trusted.overlay.whiteout` or
character devices with 0/0 device-number and treat them as whiteout.

When a directory is deleted that way, all files within that directory
are recursively deleted, as described in [Delete a file].

An entry is stored in the index for that directory, so catchup knows that
the directory entry as been handled.

Catchup needs to not report directory delete before having looked for
directory redirect. See the next section.

The whiteout and index entry are deleted once the directory contains
no cached entry. (This requires the cache triggering a change on the
index!)

*** OverlayFS behavior **

A whiteout for a directory is the same as a whiteout for a file, that
is, a character device with 0/0 device number or as a zero-size
regular file with the xattr `user|trusted.overlay.whiteout`

### Rename a directory

This might be tricky since a rename applies to the files at the time
of the rename only - and with a catchup, we don't know what that time
is.

The rename is then applied and reported for each file separately,
recursively, as described in [Rename a file]

A rename for a directory should be stored in the index so catchup
knows that it's been applied when it sees that entry again. Catchup
needs to not report directory delete before having looked for
directory redirect.

The whiteout and index entry are deleted once the source directory
contains no cached entry. (This requires the cache triggering a change
on the index!)


*** OverlayFS behavior **

This appears as a directory with `user|trusted.overlay.redirect`
pointing to the original location and a directory whiteout (to be
confirmed)
