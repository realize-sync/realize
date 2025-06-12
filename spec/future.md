# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## The beginnig of the Unreal {#unreal}

Let's start implementing the file cache described in [@/spec/design.md](design.md).

The design describe a system based on blobs, but for now we just have
file paths; let's get started with that and add blobs in a later step.

1. **DONE** in `UnrealCacheBlocking` and `UnrealCacheAsync`

   Define a database to store file list a [redb](https://github.com/cberner/redb/tree/master) database.
   - The database stores file availability in a table:
      key: (arena, path), arena as model::Arena, path as model::Path
      value: a struct containing, for each peer for which data is avalible: presence (available or deleted), file size (if available), mtime

   1.1 **DONE** fill the details section of [@/spec/unreal.md](unreal.md) with a good description of the database
   1.2 **DONE** implement the database, keeping it as a goal to expose it through `nfsserve` (next step)
   
   
2. Define a task that keeps the file list up-to-date for a specific peek.
 
   Algorithm:
    1. connect to a peer using forward_peer_history
    2. if it fails, wait according to policy
       `ExponentialBackoff::from_millis(500).max_delay(Duration::from_secs(5 *
       60))` and go back to 1. If it suceeds, move on to 2.
    3. listens to `Notification::Link` and `Notification::Unlink` and apply them to the database
    4. when `forward_peer_history` is disconnected (the join handle fails), go back to point 1
    
   This algorithm runs forever or until it is shut down. To shutdown,
   keep a `shutdown_tx: broadcast::Sender<()>`. See the way `Server`
   is implemented in
   [@crate/realize-lib/src/network.rs](../crate/realize-lib/src/network.rs)
   for an example of such a task that has a shutdown method.
   
   2.1 Design, implement and unit test the proposed task into
   /crate/realize-lib/src/logic/cache_updater.rs
    
3. Make the file list available through [nfsserve](https://github.com/xetdata/nfsserve). Don't bother serving file content just yet; reading file data should just fail, but listing files should work.

    - use the uid and gid of the daemon process for the files and directories
    - for files, use mode u=rw,g=rw,o=r
    - for directories, use mode u=rwx,g=rwx,o=rx

  The code for exposing the cache should go into the library crate
  /crate/realize-fs/src/storage/fs.rs and should be tested using the
  unit test /create/realize-fs/tests/nfsserve_test.rs

  2.1 fill the details section of [@/spec/unreal.md](unreal.md) with a good description how this will work
  2.2 write the integration in /crate/realize-fs/src/storage/fs.rs and unit-test it thoroughly
  2.3 write an integration test in /create/realize-fs/tests/nfsserve_test.rs that creates a NFS service
      and connects to it using [nfs3_client 0.4](https://crates.io/crates/nfs3_client/0.4.1)

4. Serve file content through `nfsserve` by making a request for a
   range of file content using [RealStoreService] when connected. Fail
   immediately when disconnected.

  4.1 fill the details section of [@/spec/unreal.md](unreal.md) with a description of how this would go
  4.2 implement the algorithm without storing data to the database

5. Store file data (ranges) to the database, as a cache

  5.1 fill the details section of [@/spec/unreal.md](unreal.md) with a description of how this would go
  5.2 implement

## Catch up history on (re)connection {#reconnecthistory}

Let's update `History`, defined in
[@/crate/realize-lib/src/storage/real/history.rs](../crate/realize-lib/src/storage/real/history.rs)
to take into account the latest plans, [detailed in the section
#unreal of this file](#unreal)

1. **DONE* `History` should not attempt to allow multiple subscribers for a
  given arena. Spawn a task in `History::subscribe` instead of
  `History::new` that tracks the given arena and sends to the given
  channel and that's it.
  
  1.1 implement the change
  1.2 update the tests and make sure they all pass
  
2. **DONE**Have `History` optionally send `Notification::Link` for existing
   files when going through the file list for the first time. These notifications
   have a 'catchup' flag set.
  
  2.1 implement the change
  2.2 update the unit tests and make sure they all pass; existing tests disable
  catchup so they don't get unexpected notifications.
  2.3 add a unit test to make sure that existing files are reported
  
4. Cover the case where a file is deleted while `History` is going
   through the file list.
   
   There's a race condition in this case, as catchup link and unlink
   notification can be received out of order. To avoid that, buffer
   notifications while going through the files for the first time, then
   send them all at once. 
  
   4.1 implement the change 
   4.2 add a unit test that adds and removes files just after creating
   the subscriber, without waiting for it to be ready (That is,
   without waiting for `History::subscribe().await` to return; spawn
   it or use a join.)

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

Make it possible to intercept RealStoreService operations in tests to test:

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
