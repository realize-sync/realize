# The Real - Merging local data and local modifications with the Unreal


## Overview

Use [overlayfs](https://docs.kernel.org/filesystems/overlayfs.html) to
expose a merged view of the local files and [The Unreal](unreal.md)
filesystem exporting remote files. Local modifications are stored
locally by OverlayFS with metacopy=on,redirect_dir=on. This can be
turned into a history to serve to other hosts.

Note that this is a Linux-only solution. The proposed cross-platform
solution is to make The Unreal read-write and is outside
the scope of this documented.

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

### History

As OverlayFS stores renames and deletions in xattrs, it should be
enough to look at OverlayFS to figure out what local changes need to
be sent to remote files.

[inotify](https://man7.org/linux/man-pages/man7/inotify.7.html) or can
be used to make that real-time. See [an
example](https://github.com/hannobraun/inotify-rs/blob/main/examples/stream.rs)

Without OverlayFS, we can still serve history change to a listener
using inotify.

Notifications:

* Added(arena, path, blob, modification-time-from-fs)
* Removed(arena, path)
* Updated(arena, path, new-blob, modification-time-from-fs)

A peer sends history to a peer through `HistoryService` RPCs with one
call: `notify(Vec<Notification>)` With `Notification` an enum
including the notifications listed above.

(Note that The receiving peer always knows who the notification come
from the connection, so this is not included into the RPC but is
important for the receiver to know and update its tables.)

Two scenarios are possible:

1. Peer A can request history service from Peer B by connecting to its
address, sending "HRCV" as the first 4 bytes then expect
`HistoryService` RPC call from the peer for all arenas of the peer
(TODO: allow registering for arenas)

2. Peer A can send history to Peer B by connecting to its address,
sending "HSND" as the first 4 bytes then make `HistoryService` RPC
calls for all covered arenas.

This means that both listening and blind peers can share their
history. The only difference is in makes the connection.

Blind peers should connect to remove peers as soon as they have
history to share. Listening peers can just wait for connection unless
they need history from another non-connected listening peer.

In either case, two peers may stay connected for long periods of time
and the connecting peer needs to be ready to reconnect after network
interruptions.

### History from OverlayFS attrs

### Live history using inotify


