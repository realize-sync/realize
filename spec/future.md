# Future Changes to the Spec

This file lists changes that are planned but that haven't been
integrated into the spec yet.

## Implement the unoptimized algorithm {#algoimpl}

Implement the algorithm described in the section "Sync Algorithm: Move
files from A to B" in spec/design.md

Add a good module definition, as an overview. Add thorough method
description and usage examples.

The algorithm function should take two object that implement the RealizeService
trait, defined in src/model/service.rs and modified by
#[tarpc::service], one for A (source) one for B (destination). The function should
also take a tarpc::Context::context instance to pass to the
RealizeService methods when calling them (tarpc::service implicitly
adds an argument of type tarpc::Context::context to each function).

The implementation should leave out step 4, which is an optimization,
and treat files that are present in A and partially in B as in step 3,
that is, overwrite the file that's partially in B.

The tests should call the implementation of the service defined in
src/server.rs, using an in-process channel.

Review and apply any relevant Cursor rules.

1. Read:
  - spec/design.md
  - src/model/service.rs
  and keep it in mind

2. Create the new module in src/algo/move.rs

3. Add a new async function that takes a tarpc::context::Context and
   two RealizeService implementations to work on

4. Implement that function as described

5. Run "cargo check" to make sure everything compiles, fix any issues

6. Add a test for that function, use instances of RealizeServer pass
   to the function as RealizeService implementation. See src/server.rs
   and its tests for how to create such instances.

7. Run "cargo test" to make sure the tests pass, fix any issues

## Utilities for setting up a TCP transport {#tcp}

Add code for setting up an unencrypted TCP transport and built a
server and a client for it. The code for the server goes into
src/server.rs, the code for the client into src/client.rs, any code
shared by the client and server but not part of the model can do into
src/transport.rs

Review and apply any relevant Cursor rules, including (but not limited
to):
 - .cursor/rules/rust-error-handling.mdc
 - .cursor/rules/rust-type-system.mdc
 - .cursor/rules/rust-safety.mdc
 - .cursor/rules/rust-documentation.mdc

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
