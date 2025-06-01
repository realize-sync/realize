# The Unreal - A partial local cache of remote files as FS

## Overview

Export a filesystem using
[nfsserve](https://github.com/xetdata/nfsserve) based on a
[redb](https://github.com/cberner/redb/tree/master) database and local
(blob) storage.

The filesystem works as a cache for remote data. It caches all files
available on remote peers and makes them visible through the FS. When
a file is accessed, it is downloaded and added into the cache.

Initial implementation is read-only and rely on another filesystem
storing any local changes. See [The Real](real.md).

This will be extended later into a read-write implementation that
stores local modifications for MacOS and Windows support.

## Details

TODO

### Database storage

Remote peer share a list of files they have available in full and, for
each file, the hash and size of its content (blob).

The database stores:

- Paths: path -> peer(s) to connect to, blob, size
- Blobs: hash -> reference count + (set of blob hash)|(path of block)

Blob Hash are built as a Merkle tree, that is, files are split into
4Mb blocks which are hashed separately, then (size + hashes in order)
are hashed again to get the hash of the containing blob and so on
until we get only one hash.

Blocks are stored on disk, as a file whose name is the hash.

When the reference count of a blob reaches 0, it can be deleted.
Reference count can be > 1 if the same data is present in multiple
files.

While redb is crash resistant and should guarantee reference count
stay up-to-date, external files might be lost or left over. Some
periodic GC and file content check passes might be necessary.
