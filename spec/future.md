# Future Changes to the Spec

This file lists planned changes.

## Write the daemon code {#daemon}

Implement the server "realized" in "src/bin/realized.rs". The server
should be a command-line tool that work as described in the section
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

## Throttle a TCP connection {#throttle}

The bytes sent per second in a specific TCP connection should be
limited. This should be done by making a RPC call that sets the upper
limit in bytes per second for the bytes they *send*.

The limits are set by the `realize` command on both RPC services and
specified as download and upload limits on the command-line:
--throttle 1M to throttle both to 1M/s, --throttle-up 1M
--throttle-down 512k to throttle upload to 1M/s and download to
512k/s.

