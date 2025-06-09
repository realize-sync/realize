# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Add mtime to Unlink notification {#unlinktime}

We need mtime available in notifications to make sure things are in
the right order, especially when mixing calls to RealizeService::list
with notifications that might happen while the list is being prepared.

Notification::Unlink, defined in
[@/crate/realize-lib/src/storage/real/history.rs](../crate/realize-lib/src/storage/real/history.rs)
should have a new field `mtime: SystemTime`. 

To find it, do the following:
- take the metadata of the containing directory, if it's there, use the directory mtime as mtime for unlink
- if the metadata of the containing directory could be accessed, take the metadata of the deleted file, if it's there, don't send the notification (presumably the file has been re-created)
- if the metadata of the containing directory could not be accessed, take the metadata of its parent
- if it's there, use the mtime as mtime
- if it's not there, try the parent parent... and so one until the Arena root is reached and if that path isn't available, return an error

Write the code and make sure it compiles.

To test this, first extend the unit tests that already send unlink.
Pay attention to delete_existing_dir_recursively as this is a test
where finding a parent dir's mtime might be difficult since at the
time the notification is handled, an entire tree might have been
deleted.

Fix the tests and make sure they all pass.

## Add mtime to RealizeService::list {#listtime}

The SyncedFile struct defined in
[@/crate/realize-lib/src/network/rpc/realize.rs](../crate/realize-lib/src/network/rpc/realize.rs)
should take an extra field `mtime: SystemTime` which mirrors the mtime
field used in `Notification` in
[@/crate/realize-lib/src/storage/real/history.rs](../crate/realize-lib/src/storage/real/history.rs)

In the implementation, in
[@/crate/realize-lib/src/network/rpc/realize/server.rs](../crate/realize-lib/src/network/rpc/realize/server.rs),
to get the mtime of a file, take the modification time from its
metadata with `metadata.modified().expect("OS must support mtime")`

Fix the tests, extend list_files_and_partial to check the mtime, make
sure all tests pass.


## The beginnig of the Unreal {#unreal}

Let's start implementing the file cache described in [@/spec/design.md](design.md).

The design describe a system based on blobs, but for now we just have
file paths; let's get started with that and add blobs in a later step.


1. Fetch list of remote files using RealizeService::list and store it in a [redb](https://github.com/cberner/redb/tree/master) database. 
   - The database stores file availability in a table: 
      key: (arena, path), arena as model::Arena, path as model::Path
      value: a struct containing, for each peer for which data is avalible: presence (available or deleted), file size (if available), mtime
      
   1.1 fill the details section of [@/spec/unreal.md](unreal.md) with a good description of the database
   1.2 implement the database, keeping it as a goal to expose it through `nfsserve` (next step)

2. Make the file list available through [nfsserve](https://github.com/xetdata/nfsserve). Don't bother serving file content just yet; reading file data should just fail, but listing files should work.

    - use the uid and gid of the daemon process for the files and directories
    - for files, use mode u=rw,g=rw,o=r 
    - for directories, use mode u=rwx,g=rwx,o=rx
    
  2.1 fill the details section of [@/spec/unreal.md](unreal.md) with a good description how this will work
  2.2 make the file list in the database created in step 1 available through NFS in the daemon

3. Track changes made remotely, so the filesystem view is up-to-date (Using HistoryService). Keep a connection to all listening peers, as defined in the peer list. Reconnect as necessary. 
    Algorithm:
    
     For all peers for which an address is known in PeerConfig,
     
     1. connect to the peer using a client from [@/crate/realize-lib/src/network/rpc/realize/client.rs](../crate/realize-lib/src/network/rpc/realize/client.rs)
     2. once connected, as reported by `ClientOptions::connection_events` connect to same peer using `forward_peer_history` defined in [@/crate/realize-lib/src/network/rpc/history/server.rs](../crate/realize-lib/src/network/rpc/history/server.rs)
     3. once `forward_peer_history` has returned, fetch the file list using `RealizeService::list`, using a client from [@/crate/realize-lib/src/network/rpc/realize/client.rs](../crate/realize-lib/src/network/rpc/realize/client.rs) and update the database
     4. whenever a file change notification is received, update the database (note that this can happen before the files list has been read. Use mtime to resolve conflicts)
     5. when disconnected, as reported by `ClientOptions::connection_events`, let `forward_peer_history` shut down and go back to point 2, waiting for a reconnection
     
  3.1 fill the details section of [@/spec/unreal.md](unreal.md) with a good description of the algorithm and how it all fits together
  3.2 implement the algorithm

4. Serve file content through `nfsserve` by making a request for a
   range of file content using [RealizeService] when connected. Fail
   immediately when disconnected.

  4.1 fill the details section of [@/spec/unreal.md](unreal.md) with a description of how this would go
  4.2 implement the algorithm without storing data to the database
  
5. Store file data (ranges) to the database, as a cache

  5.1 fill the details section of [@/spec/unreal.md](unreal.md) with a description of how this would go
  5.2 implement

## File hash as as Merkle tree {#merkle}

For file hashes, build a Merkle tree:
  https://docs.kernel.org/filesystems/fsverity.html

- it can be built in chunks, in parallel
- in case of mismatches, it can pinpoint where the mismatch happened
- even incomplete trees are useful (just the root, or at depth N)
- file digest = blob identifier

## Allow : in model:Path {#colon}

Forbidding just brings trouble on Linux.

## Design: Multi-peer syncing {#multi-peer}

This needs more thoughts: do peer get told about non-local (indirect)
changes? do all peers need to be told about a non-local change?

What does it look like to add a new peer?

## Gate copy by file size {#sizegate}

Instead of allowing one file to copy at a time, allow multiple for a total of up to CHUNK_SIZE bytes. A file > CHUNK_SIZE gets copied one at a time, but smaller files can be grouped together. Might be worth increasing the number of parallel files for that.

## IPV6 + IPV4 {#ipv64}

Localhost is currently resolved to ipv6 address, which isn't what's
expected in the tests, so all tests use 127.0.0.1.

This isn't right; it should be possible to specify localhost (or any
address that resolves to both an ipv6 address and an ipv4 address) and
have it work normally (try ipv6, fallback to ipv4).

This is normally automatic, I expect, but the custom transformation to
SocketAddr screws that up.

## Test retries {#testretries}

Make it possible to intercept RealizeService operations in tests to test:

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

## Implement retries {#retry}

See the section "Error and retries" of spec/design.md.

## Design and add useful logging to realized {#daemonlog}

Think about what should be logged at the error, warning info and debug
level. What format should be followed. What information should be
sent.

## Optimize reads {#readopt}

Since move_files works on multiple files at once:
 - there's opportunity to write/read for one file while another file computes hash
 - the write and reads are scattered, making it hard on the OS buffers

Experiment with different ways of organizing the work that maybe speed
things up.

