# Realize - Syncing files

## Ideas

Keep a service running on a machine that manages and mirrors a
specific directory. Algorithms work by through the service interface,
without knowing whether it's remote, local or in-process.

Algorithm described here is syncing. A specific machine owns a
specific directory. Any change made to other machines must eventually
be applied to the owner's copy or be rejected. Other machines may
*either* accept new file (file drop) (a Source) or serve, cache and
keep copies of the Owner's files (a Copy).

File copy and moves are restartable and keep partial files to avoid
re-uploading content, using the rsync approach to sending out patches.
Files are checked after being copied and fixed also without having to
re-upload everything.

## Overview

### Scenario: Move from A to B

Host B owns directory with id "synceddir". New files are added to the
copy of that directory in Host A and must be moved to Host B.

Host B runs the service as a deamon:

```bash
realized --address localhost:9771 --privkey private_key_b.key --config config
```

`private_key_b.key` is a file containing the private SSL key of the
daemon on Host B, containing a PEM-encoded private key generated using
the ed25519 algorithm.

`config` is a YAML file that contains:

- the mapping from directory id to local path

- the mapping from peer id to its public SSL key for all peers allowed
  to connect to this daemon.

```yaml

dirs:
 - directory_id1: /store/b/dir
 - directory_id2: /store/c/dir

peers:
 - peer1: | # public key as a string (PEM-encoded ED25519 Public Key)
 -----BEGIN PUBLIC KEY-----
 MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAw1603u9y/gRa194d
 ... (rest of the base64-encoded public key) ...
 -----END PUBLIC KEY-----

 - peer2: ... # public key as a string (PEM-encoded ED25519 Public Key)
```

Host A runs the realize command-line tool with the following arguments:

```bash
realize --privkey private_key_a.key --serverkey public_key_b.pem --address hostb:9771 synceddir /store/a/dir
```

--privkey `private_key_a.key` is a file containing the private SSL key
of Host A containing a PEM-encoded ED25519 private key. Host B must
have the equivalent public key in its config file or the connection
will be rejected.

--serverkey `public_key_b.key` is a file containing the public SSL key
(SPKI) of Host B, containing a PEM-encoded ED25519 public key. Host B
must have been run with the equivalent private key or the connection
will be rejected.

--address `hostb:9771` is the host and port of the daemon on Host B to
connect to

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

This algorithm is written in such a way that in can restart if
interrupted without losing work. See the section "Errors and retries"
for more details on restartable

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

  Compute a hash (BLAKE2) of Path (as reported in `SyncedFile`)

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

The `realized` daemon is the long-running background service that
manages and synchronizes one or more directories on a host. It exposes
the RealizeService RPC interface over a secure TCP connection and
enforces access control based on configured peer keys.

#### Command Line Arguments

```
realized --address <host:port> --privkey <private_key_file> --config <config_file>
```

- `--address <host:port>`: TCP address to listen on for incoming RPC connections (default: localhost:9771).
- `--privkey <private_key_file>`: Path to the PEM-encoded ED25519 private key file for this daemon.
- `--config <config_file>`: Path to the YAML configuration file specifying directory mappings and allowed peers.

#### Inputs

- **Private Key File**: PEM-encoded private key for TLS server authentication.
- **Config File**: YAML file containing:
  - `dirs`: Mapping from directory IDs to local paths.
  - `peers`: Mapping from peer IDs to their PEM-encoded ED25519 public keys.

Example config:

```yaml
dirs:
  - synceddir: /store/b/dir
  - otherdir: /store/b/other
peers:
  - peer1: |
      -----BEGIN PUBLIC KEY-----
      ...
      -----END PUBLIC KEY-----
  - peer2: ...
```

#### Outputs

- **Logs**: Diagnostic and audit logs (if enabled via RUSTLOG), including file finalization and deletion events.
- **Exit Codes**:
  - `0`: Success (clean shutdown)
  - `1`: Error (e.g., invalid config, failed to bind port, etc.)
- **Errors**: Printed to stderr for user-visible failures (not just logged).

#### Behavior

- Starts an RPC server on the specified address, using TLS with the provided private key.
- Loads the directory and peer configuration from the YAML config file.
- Accepts incoming connections only from peers with public keys listed in the config.
- Exposes the RealizeService API for directory listing, file transfer, and sync operations.
- Manages the local state of each configured directory, including handling partial files and file finalization.
- Logs key events (file finalization, deletion) at INFO level if logging is enabled.
- On fatal error (e.g., config parse failure, port in use), prints error to stderr and exits with code 1.
- Designed to be robust to interruptions: can be restarted without data loss or corruption.

#### Example Usage

```
realized --address localhost:9771 --privkey private_key_b.key --config config.yaml
```

This starts the daemon on address localhost:9771, using the specified private key and configuration file.

### The `realize` command

The `realize` command is a CLI tool used to initiate and control the synchronization of a local directory with a remote directory managed by a `realized` daemon. It acts as a client, connecting to the daemon over a secure TCP connection and invoking the move/sync algorithm.

#### Command Line Arguments

```
# Local to remote
realize --src-path <local_path> --dst-addr <host:port> --privkey <private_key_file> --peers <peers_pem_file> --directory-id <directory_id>

# Remote to local
realize --src-addr <host:port> --dst-path <local_path> --privkey <private_key_file> --peers <peers_pem_file> --directory-id <directory_id>

# Both remote
realize --src-addr <host:port> --dst-addr <host:port> --privkey <private_key_file> --peers <peers_pem_file> --directory-id <directory_id>

# Both local
realize --src-path <local_path> --dst-path <local_path>

# Metrics (optional)
realize ... [--metrics-addr <host:port>] [--metrics-pushgateway <url>] [--metrics-job <job>] [--metrics-instance <instance>]
```

- `--src-path <local_path>`: Local source directory path
- `--dst-path <local_path>`: Local destination directory path
- `--src-addr <host:port>`: Source remote address
- `--dst-addr <host:port>`: Destination remote address
- `--privkey <private_key_file>`: Path to the PEM-encoded private key file (required for remote)
- `--peers <peers_pem_file>`: Path to PEM file with one or more peer public keys (required for remote)
- `--directory-id <directory_id>`: Directory id (optional, defaults to 'dir' for local/local)
- `--throttle <rate>`: Throttle upload and download (e.g. 1M)
- `--throttle-up <rate>`: Throttle upload (e.g. 1M)
- `--throttle-down <rate>`: Throttle download (e.g. 512k)
- `--max-duration <duration>`: Maximum total duration for the operation. Accepts human-readable durations (e.g. "5m", "30s"). Parsed as `humantime::Duration`. If the operation does not complete within this time, an error is printed to stderr and the process exits with status code 11.
- `--metrics-addr <host:port>`: (Optional) Address to bind a Prometheus metrics HTTP endpoint (pull mode)
- `--metrics-pushgateway <url>`: (Optional) URL of Prometheus Pushgateway to push metrics at the end of the run (push mode)
- `--metrics-job <job>`: (Optional) Job name for Prometheus metrics (default: "realize")
- `--metrics-instance <instance>`: (Optional) Instance label for Prometheus metrics

#### Inputs

- **Private Key File**: PEM-encoded private key for TLS client authentication (required for remote endpoints).
- **Peers File**: PEM file containing one or more public keys for the servers that the client can connect to (required for remote endpoints).
- **Directory ID**: Identifier for the directory to sync (optional for local/local, required otherwise).
- **Local Path**: Path to the local directory to be synchronized (for local endpoints).
- **Remote Host/Port**: Network address of the remote `realized` daemon (for remote endpoints).
- **Metrics Options**:
  - `--metrics-addr`: If set, realize will serve Prometheus metrics on the given address (pull mode)
  - `--metrics-pushgateway`: If set, realize will push metrics to the given Pushgateway URL at the end of the run (push mode)
  - `--metrics-job`: Job name for metrics (default: "realize")
  - `--metrics-instance`: Instance label for metrics (optional)

#### Outputs

- **Progress Output**: Progress information is printed to stdout (using indicatif), showing sync status per file and overall.
- **Errors**: Any errors are printed to stderr. Logging is not used for user-facing errors.
- **Prometheus Metrics**: If metrics options are set, realize will export Prometheus metrics via HTTP (pull) or push to a Pushgateway (push).
- **Exit Codes**:
  - `0`: Success (all files synced as intended)
  - `1`: Error (e.g., connection failure, sync error, invalid arguments)
  - `11`: Timed out (--max-duration exceeded)
  - `12`: Succeeded, but failed to push metrics
  - `20`: Interrupted (Ctrl-C)

#### Behavior

- Parses command line arguments and validates inputs.
- Connects to the specified `realized` daemon(s) using TLS, authenticating with the provided private key and verifying peers.
- Starts an in-process RealizeService instance for local directories.
- Invokes the move/sync algorithm to synchronize files between the source and destination.
- Reports progress to stdout, including per-file and overall sync status.
- If metrics options are set, exports Prometheus metrics (see above).
- On success, exits with code 0. On error, prints a message to stderr and exits with code 1. If the operation times out due to --max-duration, prints an error message to stderr and exits with code 11.
- Designed to be restartable and robust to interruptions; re-running the command resumes any incomplete sync.

### Errors and retries

If communication with the remote host is lost, this command retries
with exponential backoff (5s, 15s, 30s, 1m, 2m, 5m, then always 5m)
until communication can be re-established or duration set with
--max-duration is reached. If --max-duration is exceeded, the command
prints an error message to stderr and exits with status code 11.

Retries only apply to network errors. They don't apply to the
following errors:
 - Local errors
 - App errors (errors embedded into RealizeServiceResponse)
 - Disk I/O errors (either)
 - Authentication error (communication not authorized)

Open issues:

- at which level should retry apply. If retrying after time has
  passed, it might be worth retrying the whole move_files operation.
  If just reconnecting, it might be enough to do it inside the loop in
  tcp.rs.

- how to deal with service::Config ? Ideally, it or the limiter should
  be conserved with retries without having to do anything.

#### Example Usage

```
realize --privkey private_key_a.key hostb:9771 synceddir /store/a/dir
```

This command syncs the local directory `/store/a/dir` with the remote
directory `synceddir` managed by the daemon at `hostb:9771`, using the
provided private key for authentication.

*Example* output of a successful command:

```
[1/4] Sent subdir1/file1.txt 3G BLAKE2 A8382919827839811893
[2/4] Sent subdir1/file2.txt 100M BLAKE2 AE0031343989EAA7888F
[4/3] Stored   ~/Downloads/image3.png as 6f6f6f3d19999999.jpg
Success 2 files imported
```

Note: everything is sent to stdout.

*Example* output of a command in progress:

```
[1/4] Sent subdir1/file1.txt 3G BLAKE2 A8382919827839811893
[2/4] Sent subdir1/file2.txt 100M BLAKE2 AE0031343989EAA7888F
[3/4] Sending [=======>                    ] (300 k/s) subdir1/file3.txt 2.5G
[4/4] Sending [============>               ] (150 k/s) subdir1/file4.txt 110M
[4/3] Stored   ~/Downloads/image3.png as 6f6f6f3d19999999.jpg
Success 2 files imported
```

Note: everything is sent to stdout.

*Example* output of the same command, once finished and failed:

```
[1/4] Sent   subdir1/file1.txt 3G BLAKE2 A8382919827839811893
[2/4] Sent   subdir1/file2.txt 100M BLAKE2 AE0031343989EAA7888F
[2/4] Sent   subdir1/file3.txt 2.5G BLAKE2 001030E77AA710033331
[3/4] Failed subdir1/file3.txt 2.5G: I/O error: disk full
Error 1 file failed.2 files imported
```

Note: the lines "[3/4] Failed ..." and "Error ..." and sent to stderr,
the rest to stdout.

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

A conflict is detected if the checksum (BLAKE2) of a file on A and B
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

Keys must use the ed25519 algorithm and be in the PEM format.

Example command-line to generate a key pair for a peer (client or
server) using openssl. The private key goes into `peer.key` and the
public key into `peer-spki.pem`

``bash
openssl genpkey -algorithm ed25519 -out peer.key
openssl pkey -in peer.key -pubout -out peer-spki.pem
```

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

  - the size and BLAKE2 hash of the file

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

