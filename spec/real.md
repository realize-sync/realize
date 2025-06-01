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

inotify or fanotify should be used to make that real-time.


