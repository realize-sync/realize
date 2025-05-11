# Future Changes to the Spec

This file lists planned changes.

## Write the daemon code {#daemon}

Implement the server "realized" in "src/bin/realized.rs". The server
should be a command-line tool that work as described in the section
"The `realized` daemon" of spec/design.md, put it into
src/bin/realized.rs as specified in the section "Code organization" of
spec/design.md.

The server code should:
 - init env_logger to log nothing unless the env variable is set
 - parse the command line arguments using clap
 - parse the YAML config file
 - create a TCP server with authentication (see src/server.rs and src/tcp.rs)
   using the provided private key and all public key in the config
   file
 - run that server forever or until the daemon is interrupted

Display any error to stdout. On success, exit with status 0. On error,
display the error to stderr, exit with status 1. (It's not enough to
log the error)

Review and apply any relevant Cursor rules.

1. Write the server code in src/bin/realized.rs, putting in code that
   parses the command line using clap (use derive feature).

2. Implement the server using the tools from the library crate,
   notably src/server.rs and src/tcp.rs.

3. Run "cargo check" to make sure everything compiles, fix any errors

4. Write an integration test in test/daemon_integration_test.rs that
   starts the server and calls its List method from a client created
   in the test, then makes sure the result is as expected.

## Write the command line code {#cli}

Implement the command-line tool "realize" in src/bin/realize.rs. See
that command described in the section "The `realize` command" of
spec/design.md, put it into src/bin/realize.rs as specified in the
section "Code organization" of spec/design.md.

The command-line tool should:

 - init env_logger to log nothing unless the env variable is set

 - parse the command-line arguments using clap

 - connect to an instance of RealizeService using TCP at the given
   address with the given private key and the server public key as
   only peer in the PeerVerifier.

 - start an in-process instance of RealizeService for the directory and
   directory id given in the command-line arguments (See
   RealizeServer::as_inprocess_client)

 - call move_files on the two RealizeServiceClient instances from src/algo.rs

 - report success with exit status code 0, write any error to stderr
   and exit with status code 0. (Note that logging is not enough to
   report errors.)

On success, exit with status 0. On error, display the error to stderr,
exit with status 1. (It's not enough to log the error)

Review and apply any relevant Cursor rules.

Review and apply the relevant sections of spec/design.md, including
but not limited to the sections "Details" "Service Definition", "Code
organization" and "Implementation and Dependencies"


1. Write the server code in src/bin/realize.rs, putting in code that
   parses the command line that clap (use derive feature).

3. Implement the cli using the tools from the library, as described
   above.

4. Run "cargo check" to make sure everything compiles, fix any errors

5. Write an integration test in
   crate/daemon/test/daemon_integration_test.rs that starts the server
   using TCP transport, the use the command-line tool on it, then
   makes sure files were moved from one directory to the other as
   expected.

## Throttle a TCP connection {#throttle}

The bytes sent per second in a specific TCP connection should be
limited. This should be done by making a RPC call that sets the upper
limit in bytes per second for the bytes they *send*.

The limits are set by the `realize` command on both RPC services and
specified as download and upload limits on the command-line:
--throttle 1M to throttle both to 1M/s, --throttle-up 1M
--throttle-down 512k to throttle upload to 1M/s and download to
512k/s.

