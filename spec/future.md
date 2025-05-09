# Future Changes to the Spec

This file lists planned changes.

## Update copy_files to take a RealizeClient {#copy_file_client}

copy_files() should always work on tarpc clients, not directly on
RealizeService instances.

- add a test that uses copy_files on two RealizeClient RPC instances
  built using an in-process channel. See the RPC test in src/server.rs
  for how to do that.

- run "cargo test", fix any issues

## Tell RealizeService.send about a final chunk {#sendfinal}

So send can truncate the file if it is larger.

- update RealizeService::send to take an argument saying it is final
- update RealizeServer::send implementation to truncate file to the final size
- add test that sets up a file that's larger and is truncated by the send method
- run "cargo test" and fix any issues

## Use rayon to parallelize copy_files {#rayon}

- rewrite copy_files to use iterators instead of for loops

- adapt logic to use rayon, so it can parallelize. Make sure to Run
  things like listing files on both source and dest in parallel.

## Transform copy_files into move_files {#move}

1. Add the following methods to RealizeService:
 - compute sha-256 hash
 - check sha-256 hash
 - delete a file
 See the section "Service Definition" in spec/design.md

2. Implement the new methods in RealizeServer and test them.

3. Transform copy_files into move_files in src/algo.rs
 - implement step 6 described in section "Sync Algorithm: Move files from A to B" in spec/design.md
   use the newly added methods
 - rename copy_files to move_files and copy_file to move_file
 - update the test to make sure the files are now moved and not just copied
 - add a test to make sure that a final file that was different in the destination is updated

## Setup a TCP transport for RealizeService {#tcp}

Add code for setting up an unencrypted TCP transport and built a
server and a client for it. The code for the server goes into
src/server.rs, the code for the client into src/client.rs, any code
shared by the client and server but not part of the model can do into
src/transport.rs

Review and apply any relevant Cursor rules.

Review and apply the relevant sections of spec/design.md, including
but not limited to the sections "Details" "Service Definition", "Code
organization" and "Implementation and Dependencies"

1. Implement functions to build a server and client using a TCP
   transport

2. Run "cargo check" to make sure everything compiles, fix any issues

3. Write tests to test the server and client, make sure they can
   communicate using localhost as an address.

4. Run "cargo test" to make sure all test pass, fix any issues

## Write the daemon code {#daemon}

Implement the server "realized" in "src/bin/realized.rs". The server
should be a command-line tool tat work as described in the section
"Overview" of spec/design.md

Leave out anything that has to do with security and authentication for
now. The server just exposes a public service available through TCP at
the port it's given.

The server code should parse the command line arguments, call the code
from the library (src/server.rs), process the result. On success, exit
with status 0. On error, display the error to stderr, exit with
status 1. (It's not enough to log the error)

Review and apply any relevant Cursor rules.

Review and apply the relevant sections of spec/design.md, including
but not limited to the sections "Details" "Service Definition", "Code
organization" and "Implementation and Dependencies"

1. Write a description of the command into spec/realized_man.md,
   format it like a UNIX manpage, based on its description in
   spec/design.md. Include usage examples and command output.

2. Keeping spec/realized_man.md and spec/design.md in mind, write the
   server code in src/bin/realized.rs, putting in code that parses the
   command line that clap (use derive feature).

2. Implement the server using the tools from the library crate,
   notably src/server.rs. Put any code that's needed to setup
   a TCP transport into src/server.rs and *not* in bin/*.rs

3. Run "cargo check" to make sure everything compiles, fix any errors

4. Write an integration test in
   test/daemon_integration_test.rs that starts the
   server and calls its List method, then makes sure the result is
   as expected.

## Write the command line code {#cli}

Implement the command-line tool "realize" in src/bin/realize.rs. See
that command described in the section "Overview" of spec/design.md


The command-line tool should:

- connect to an instance of RealizeService using TCP at the given address

- start an in-process instance of RealizeService for the directory and
  directory id given in the command-line arguments

- call the move algorithm from src/algo/move.rs

- report success with exit status code 0, write any error to stderr
  and exit with status code 0. (Note that logging is not enough to
  report errors.)

The server code should parse the command line arguments, call code
from the library to connect to the server using TCP, calling
src/client.rs, create an in-process service instance using
src/server.rs, call the move algo from src/algo/move.rs on both
instance, process the result.

On success, exit with status 0. On error, display the error to stderr,
exit with status 1. (It's not enough to log the error)

Review and apply any relevant Cursor rules.

Review and apply the relevant sections of spec/design.md, including
but not limited to the sections "Details" "Service Definition", "Code
organization" and "Implementation and Dependencies"


1. Write a description of the command into spec/realize_man.md,
   format it like a UNIX manpage. Include usage examples and command
   output.

2. Keeping specs/design.md and spec/realize_man.md in mind, write the
   server code in src/bin/realize.rs, putting in code that
   parses the command line that clap (use derive feature).

3. Implement the cli using the tools from the library, as described
   above. Keep any code that's needed to setup the service in
   the library, extending it if necessary, and *not* in bin/*.rs.

4. Run "cargo check" to make sure everything compiles, fix any errors

5. Write an integration test in
   crate/daemon/test/daemon_integration_test.rs that starts the
   server and calls its List method, then makes sure the result is
   as expected.

## Design Review Findings {#design_review}

This section lists issues identified in the design doc (spec/design.md) and proposed updates. Each item is numbered for future reference.

### Issues and Proposed Updates

1. **Conflict Resolution**: Add a section on how file conflicts are detected and resolved (e.g., last-writer-wins, manual intervention).
2. **Partial File Handling**: Clarify the lifecycle and cleanup of partial files (e.g., `.part`), including recovery from interrupted or failed transfers.
3. **Error Handling and Retries**: Add a section describing error handling strategy, including which errors are retried and how.
4. **Directory Structure Sync**: Specify which file types are supported (e.g., regular files, directories, symlinks) and how directory structure is mirrored.
5. **Security Details**: Expand the security section to cover key management, rotation, and revocation procedures.
6. **Scalability and Performance**: Add notes on expected performance, bottlenecks, and any planned optimizations for large numbers of files or directories.
7. **Configuration File Format**: Add a formal schema or comprehensive example for the config file, listing all possible fields.
8. **API Versioning**: Add a note on API versioning and backward compatibility.
9. **Logging and Monitoring**: Add a section on operational monitoring, log levels, and metrics.
10. **Testing Strategy**: Add a brief section describing the overall testing approach (unit, integration, end-to-end).
11. **Ownership Model**: Clarify how directory ownership is established and enforced.
12. **Sync Directionality**: Clarify if sync is always unidirectional or can be bidirectional.
13. **Service Discovery**: Clarify how peers are discovered and connected (manual config or discovery mechanism).
14. **File Deletion Semantics**: Specify file deletion propagation rules (e.g., what happens if a file is deleted on A or B).
15. **Progress Reporting**: Clarify what progress information is shown to the user (per file, per directory, etc.).
16. **CLI Path Arguments**: Fix inconsistencies in CLI examples and comments regarding directory paths.
17. **Partial File Naming**: Standardize and clarify the naming convention for partial files.
18. **Key File Naming**: Fix inconsistencies in comments and examples regarding key file naming.

Refer to this section for future design updates and clarifications.
