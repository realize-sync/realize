# Realize - Symmetric File Syncer

## Ideas

Keep a service running on a machine that manages and mirrors a
specific directory. Algorithms work by through the service interface,
without knowing whether it's remote, local or in-process.

Algorithm described here is syncing. A specific machine owns a
specific directory. Any change made to other machines must eventually
be applied to the owner's copy or be rejected. Other machines may or
may not keep cached copies.

### Future

While the implementation and this design doc concentrates on syncing,
this is meant to be extended later extended later to read/write to
make available a union view of the directory on different machines,
trough [FUSE](https://github.com/cberner/fuser) or
[nfsserve](https://github.com/xetdata/nfsserve).

## Overview

### Scenario 1: Move from A to B

Host B owns directory with id "synceddir". New files are added to the
copy of that directory in Host A and must be moved to Host B.

Host B runs the service as a deamon:

```bash
realized --port 9771 --privkey private_key_b --config config
```

`private_key_b` is a file containing the private SSL key of the daemon on Host B.

`config` is a file that contains:

- the public SSL key of all peer allowed to connect to this daemon.
- the ID and local path of all synced directories

```yaml
peers:
 - a: ... # public key
dirs:
 - synceddir: /store/b/dir
```

Host A runs the realize command-line tool with the following arguments:

```bash
realize --privkey private_key_a hostb:9771 synceddir /store/a/dir
```

`private_key_a` is a file containing the private SSL key of Host B
(Host B has the equivalent public key)

`hostb:9771` is the host and port of the daemon on Host B to connect
to

`synceddir` is the directory Id to sync

`/store/b/dir` is the path on Host A containing the root of `synceddir`

This call moves any file from /store/a/dir to /store/b/dir as
described below, in Sync Algorithm: Move files from A to B.

## Details

### Components of the system

1. A library containing the code:
 - service definition
 - service implementation
 - algorithms

 and the tests for that code.

2. A tool `realized` that defines the daemon

  Only contains the code that is specific to the service, such as
  command-line parsing, output (log) and exit code.

  and integration tests for that command.

3. A tool `realize` that defines a command line tool

  Only contains the code that is specific to a command-line tool, such
  as command-line parsing and output (messages to stdout, errors to
  stderr, progress to stdout using indicatif) and exit code.

  and integration tests for that command.

### Sync Algorithm: Move files from A to B

1. list files on A // list files on B
2. compare the two list, note files:
  - present in A, not in B
  - present in A and B
3. for all files present in A, not in B:
 - copy file (may be interrupted)
4. for all files present in A and partially in B:
 - sync file from A to B using rsync best deal with big files whose transfer was interrupted

 From https://github.com/dropbox/fast_rsync:

    These functions can be used to implement an protocol for
    efficiently transferring data over a network. Suppose hosts A and
    B have similar versions of some file foo, and host B would like to
    acquire A's copy.

    Host B calculates the Signature of foo_B and sends it to A. This
    is cheap because the signature can be 1000X smaller than foo_B
    itself. (The precise factor is configurable and creates a tradeoff
    between signature size and usefulness. A larger signature enables
    the creation of smaller and more precise deltas.)

    Host A calculates a diff from B's signature and foo_A, and sends it to B.

    Host B attempts to apply the delta to foo_B. The resulting data is
    probably (*) equal to foo_A.

6. for all files now present in A an B:
 - build checksum on A // build checksum on B
 - if same, delete from A
 - if not same, make B partial (rename) then go back to 4

If file changed on A while running, either:
 - interrupt, wait, restart
 - fail (in a restartable way)

If file deleted on A while running, drop it on A and B

Each step may be interrupted. If interrupted, running the algorithm
again should continue the work that was interrupted.

Partial files should be clearly marked as partial, stored in the same
directory as the destination .<filename>.part

### Service Definition (RealizeService)

This is the service exposed by the RPC (tarpc). See Security for how
to connect to that RPC.

Methods:

- `List(DirectoryId) -> Result<Vec<SyncedFile>>`

  `DirectoryId` identifies the synced directory

  `SyncedFile` contains:

    - the path to the file, relative to the synced directory (`PathBuf`)

    - size in bytes (`usize`)

    - whether the file is complete or partial (if partial, the path is
      the final path, without the ".part" suffix or "." prefix)

- `Send(DirectoryId, Path, ByteRange, Data) -> Result<()>`

  Path is from the `SyncedFile`. This is the final path, even if the
  file is normally partial. If the file is not partial, it is made
  partial by this call.

  `ByteRange` describes the range of data in the file.

  Data is the binary data, a [u8]

- `Finish(DirectoryId, Path) -> Result<()>`

  Mark a partial file complete. Rename it. Does nothing if the file is
  already complete. Fail if the file isn't available neither as a
  complete or a partial file.

- `CalculateSignature(DirectoryId, Path, ByteRange) -> Result<Some(Signature)>`

  `Signature` (from `fast_sync`) of file whole final path is Path. If the
  file is only available as partial file, the signature of the partial
  file is returned.

  Returns the signature for the range, which is missing if the range
  is missing on the file.

  The range must be chosen to get a reasonably-sized signature and
  delta to send in a RPC and keep in memory.

- `Diff(DirectoryId, Path, ByteRange, Signature) -> Result<Delta>`

  Compute a delta from the file at Path (partial or complete) and a
  given Signature.

  The range must be chosen to get a reasonably-sized delta to send in
  a RPC and keep in memory.

- `ApplyDelta(DirectoryId, Path, ByteRange, Delta) -> Result<()>`

  Apply the delta to the given range of the given path. If the file is
  not partial, it is made partial by this call. If the file is smaller
  than the range, it is expanded (any hole is filled with 0).

  ByteRange specifies the size of the data to be written. A bigger or
  smaller Delta must be rejected.

- `Hash(DirectoryId, Path)  -> Result<Hash>`

  Compute a hash (SHA-256) of Path (partial or complete)

### Security

The server listens for RPC calls on a TCP port (Can be configured.
Defaults to 9771).

Connection must be made using TLS with a client key known to the
server and a server key known to the client (no certificate
authority).

### Implementation and Dependencies

Implemented using rust

- Continue interrupted syncing of large files:
  https://crates.io/crates/fast_rsync version 0.2

- Verify files after sync, before delete:
  https://crates.io/crates/sha2 version 0.10

- RPC: https://crates.io/crates/tarpc version 0.36

- Security: https://crates.io/crates/rustls through tokie_rultls
  Example: https://github.com/google/tarpc/blob/master/tarpc/examples/tls_over_tcp.rs

- Command-line parsing: https://crates.io/crates/clap (with derive
  feature) version 4.5

- Progress output on the command line:
  https://crates.io/crates/indicatif version 0.17

- Errors: https://crates.io/crates/anyhow version 1.0,
          https://docs.rs/thiserror/latest/thiserror/ version 2.0

- Logging: env_logger version 0.11

  (Note, however, that logging should only be for debugging the
  library. Command-line output and printing errors must be done by the
  command.)

- async, task, network connections and futures: tokio version 1.45

- Testing utilities (as needed):

  - assert_fs v1.1: fixtures (tempdir) and assertions to work with
    files

  - dir-diff v0.3: compare the content of two directories

  - assert_cmd v2.0: running a command and checking its status and
    output, for integration tests

  - predicates-rs v3.0: general predicates

  - assert_unordered v0.3: assert for comparing containers

