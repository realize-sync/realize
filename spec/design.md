# Realize - Symmetric File Syncer

## Ideas

Keep a service running on a machine that manages and mirrors a
specific directory. Algorithms work by through the service interface,
without knowing whether it's remote, local or in-process.

Algorithm described here is syncing. A specific machine owns a
specific directory. Any change made to other machines must eventually
be applied to the owner's copy or be rejected. Other machines may
*either* accept new file (file drop) (a Source) or serve, cache and
keep copies of the Owner's files (a Copy).

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
realized --port 9771 --privkey private_key_b.key --config config
```

`private_key_b.key` is a file containing the private SSL key of the
daemon on Host B, containing a PEM-encoded private key.

`config` is a YAML file that contains:

- the mapping from directory id to local path

- the mapping from peer id to its public SSL key for all peers allowed
  to connect to this daemon.

```yaml

dirs:
 - directory_id1: /store/b/dir
 - directory_id2: /store/c/dir

peers:
 - peer1: | # public key as a string (PEM-encoded Public Key)
 -----BEGIN PUBLIC KEY-----
 MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAw1603u9y/gRa194d
 ... (rest of the base64-encoded public key) ...
 -----END PUBLIC KEY-----

 - peer2: ... # public key as a string (PEM-encoded Public Key)
```

Host A runs the realize command-line tool with the following arguments:

```bash
realize --privkey private_key_a.key hostb:9771 synceddir /store/a/dir
```

`private_key_a.key` is a file containing the private SSL key of Host B
(Host B has the equivalent public key), containing a PEM-encoded
private key.

`hostb:9771` is the host and port of the daemon on Host B to connect
to

`synceddir` is the directory Id to sync

`/store/a/dir` is the path on Host A containing the root of `synceddir`

This call files from /store/a/dir to /store/b/dir as described below,
in the section "Sync Algorithm: Move files from A to B".

## Details

### Components of the system

1. A library containing the code:
 - service definition
 - service implementation
 - algorithms

 and the tests for that code.

2. A tool `realized` that defines the daemon (see section "The realized daemon")

  Only contains the code that is specific to the service, such as
  command-line parsing, output (log) and exit code.

  and integration tests for that command (see section "The realize command")

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

#### Methods

- `List(DirectoryId) -> Result<Vec<SyncedFile>>`

  `DirectoryId` identifies the synced directory

  `SyncedFile` contains:

    - the path to the file, relative to the synced directory (`PathBuf`)

    - size in bytes (`usize`)

    - whether the file is complete or partial

- `Read(DirectoryId, Path, ByteRange) -> Result<Vec<u8>>`

  Read the given range of data from path and return the result. The
  range *must* be correct.

  The file might be complete or partial; it doesn't matter to this
  call.

- `Send(DirectoryId, Path, ByteRange, FileSize, Data) -> Result<()>`

  Path is as returned by `SyncedFile`.

  `ByteRange` describes the range of data in the file.

  Data is the binary data, a [u8]

  When writing to a range that's past the current size of the file,
  the file is extended with 0.

  When writing to a file whose size is larger that FileSize, the file
  is truncated. This happen when writing the last ByteRange, that ends
  exactly at FileSize.

  It is an error for ByteRange to write past FileSize.

  If the file is in final form, it is turned back into a partial file
  before writing.

- `Finish(DirectoryId, Path) -> Result<()>`

  Mark a partial file complete. Rename it. Does nothing if the file is
  already complete. Fail if the file isn't available neither as a
  complete or a partial file.

- `CalculateSignature(DirectoryId, Path, ByteRange) -> Result<Some(Signature)>`

  `Signature` (from `fast_sync`) of file path, as reported in
  `SyncedFile`.

  Returns the signature for the range, which is missing if the range
  is missing on the file.

  The range must be chosen to get a reasonably-sized signature and
  delta to send in a RPC and keep in memory.

  The file might be complete or partial; it doesn't matter to this
  call.

- `Diff(DirectoryId, Path, ByteRange, Signature) -> Result<Delta>`

  Compute a delta from the file at a path (as reported in
  `SyncedFile`) and a given Signature.

  The range must be chosen to get a reasonably-sized delta to send in
  a RPC and keep in memory.

  The file might be complete or partial; it doesn't matter to this
  call.

- `ApplyDelta(DirectoryId, Path, ByteRange, FileSize, Delta) -> Result<()>`

  Apply the delta to the given range of the given path (as reported in
  `SyncedFile`).

  ByteRange specifies the size of the data to be written. A bigger or
  smaller Delta must be rejected.

  When writing to a file whose size is larger that FileSize, the file
  is truncated. This happen when writing the last ByteRange, that ends
  exactly at FileSize.

  It is an error for ByteRange to write past FileSize.

  If the file is in final form, it is turned back into a partial file
  before writing.

- `Hash(DirectoryId, Path)  -> Result<Hash>`

  Compute a hash (SHA-256) of Path (as reported in `SyncedFile`)

  The file might be complete or partial; it doesn't matter to this
  call.

- `Delete(DirectoryId, Path) -> Result<()>`

  Delete the path (as reported in `SyncedFile`).

  Deletes both the partial *and* complete form of the file, as both
  may exist. Succeeds if the paths already don't exist.

#### Implementation

The content of the directory is stored on each host by the process
that exposes the RPC in an arbitrary path with files that have
arbitrary ownership and permission.

For example:

<root>
├── file1.txt
├── subdir1/
│   ├── file2.txt
│   └── file3.txt
└── subdir2/
    └── file3.txt

During the copy, files are stored in a special format that marks them
as partial files. The name of partial file is "." <final file name> ".part"
and they're stored in their final directory

For example, during the copy of "file4.txt", the partial file is
stored in "<root>/subdir1/.file4.txt.part"

<root>
├── subdir1/
│   ├── .file4.txt.part
└── ...

Once the copy is finished, the file is stored in "<root>/subdir1/file4.txt"

<root>
├── subdir1/
│   ├── file4.txt
└── ...

Callers are aware that a file might be in final or partial state, but
all that is ever available in the RPC is the path to the final form of
the path relative to the root of the directory.

For example, the Send method called with DirectoryId = "mydir" and
Path = "subdir1/file4.txt" would write to
"<root>/subdir1/.file4.txt.part", assuming that "mydir" is mapped to
<root> in the configuration.

### The `realized` daemon

TODO: document the daemon described for now only in section "Scenario
1: Move from A to B" and "Components of the System".

### The `realize` command

TODO: document the command-line tool described for now only in section
"Scenario 1: Move from A to B" and "Components of the System". Include
an example output with progress.

### What is synced?

The following is synced:
 - regular files and their containing directories
 - the file content

The following is *NOT* synced and ignored by RealizeService if they
exist in the directory:
 - empty directories
 - permission and file ownership
 - special files (symlink, devices, sockets, ...)

The following is synced only when going from Owner to Source (see
"Conflict Resolution"):
 - deletion

### Conflict Resolution

Conflicts occur when a file is modified on both the source (A) and
destination (B) before a sync operation completes.

#### Conflict Detection

A conflict is detected if the checksum (SHA-256) of a file on A and B
differs after both have been modified since the last successful sync.

The sync algorithm compares file hashes after transfer. If the hashes
differ and both files have changed since the last sync, a conflict is
present.

#### Conflict Resolution Policy

Each copy of a directory is flagged with either:

- **Owner**: This is where the authoritative copy of files are
  eventually stored

- **Source**: This is where new versions of files are stored, to be
  sent to the directory Owner

- **Copy** (future): This is where copies of the Owner's files are stored

There can only be one Owner and one Source. Files on the Source
overwrites files on the owner. Files in the Copy are always
overwritten by files in the Owner.

Sync is strictly directional: from Source to Owner, from Owner to Copy.

In the `realize` command, the definition of source and owner is
implicit in the command. Future versions of the system may make that
configurable.

### Security

The server listens for RPC calls on a TCP port (Can be configured.
Defaults to 9771).

Connection must be made using TLS with a client key known to the
server and a server key known to the client (no certificate
authority).

Each host has:
 - a private key
 - a set of public keys it accepts connections from
This is configured at command startup.

### Service Discovery

There is no service discovery and there is no plan to add any. Peers
must be aware of other peers TCP address and public key to connect.

### Versioning

As long as the crate version is < 1.0, RPCs, APIs, code, configuration
files and directory structure might change at any time. No
backward-compatibility guarantees are given.

Strict versioning and backward compatibility are a requirement for
switching the crate to 1.0.

### Logging

By default, logging is disabled for all levels. Logging is enabled by
setting the env variable RUSTLOG as supported by the package
env_logger.

Note that since logging is disabled by default, logging cannot be
relied on to let users know about errors. Errors must be printed to
stderr separately, as appropriate in the current command.

The following events are logged at INFO level, with timestamp:

- making a file final, which includes:

  - the ID of the peer that made the change, as defined in the YAML
    config file, if made through RPC or "local" otherwise

  - the size and SHA-256 hash of the file

- deleting a final file, which includes:

  - the ID of the peer that made the change, as defined in the YAML
    config file, if made through RPC or "local" othrewise

More TBD

### Monitoring

Monitoring using prometheus is supported. Full set of metrics TBD.

Some things to log:
 - general rust metrics (process health)
 - number of bytes sent, bytes received
 - number of bytes that would have be sent/received without using fast_rsync
 - number of files sent, received
 - number of bytes deleted

### Scalability and Performance

TBD

### Code organization

├── Cargo.lock
├── Cargo.toml
├── src/
│   ├── lib.rs
│   ├── model.rs          - model layer
│   │   └── service.rs    - service definition
│   ├── server.rs         - service implementation
│   ├── client.rs         - client service constructor
│   ├── algo.rs           - algorithms, built on service and model layer
│   │   └── move.rs       - move algorithm
│   └── bin/
│       ├── realize.rs    - command-line interface
│       └── realized.rs   - daemon
└── tests/
    └── feature_integration_test.rs - feature-specific integration test

Note that <modulename>.rs is used to define the module <modulename>.
*NOT* <modulename>/mod.rs. See the rule "NEVER create any mod.rs file"
in .cursor/rules/no-modrs.mdc

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

- async, task, network connections and futures:
  - tokio version 1.45
  - rayon

- Testing utilities (as needed):

  - assert_fs v1.1: fixtures (tempdir) and assertions to work with
    files

  - dir-diff v0.3: compare the content of two directories

  - assert_cmd v2.0: running a command and checking its status and
    output, for integration tests

  - predicates-rs v3.0: general predicates

  - assert_unordered v0.3: assert for comparing containers

### TODO

- **Cleaning up Partial Files**: The doc describes partial file handling
  and naming, but does not specify how abandoned or failed partial
  files are detected and cleaned up.

- **Copy Role Implementation**: The "Copy" role is described as a
  future feature, but there is no detail on how it will be implemented
  or how it will interact with Owner/Source.

- **Error Handling and Retries**: There is no section describing which
  errors are retried, which are fatal, and how users are notified of
  failures.
