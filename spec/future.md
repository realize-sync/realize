# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Deadlines {#deadlines}

Let's change the way --max-duration works.

Currently --max-duration is implemented as a timer that kills the
process. Let's implement it properly this time, using deadlines. There
shouldn't be any need to kill the process. Also, the application
should display proper messaging, including the number of files let in
a partially moved state.

To do that, we want to set an overall deadline in bin/realize.rs,
controlled by --max-duration, 24h by default (because there has to be
one), then pass it to move_files, and let Reconnect (already set up in
tcp.rs) handle deadlines. Then, update the code that checks the error
and success count to detect when the deadline has been hit and change
the summary that's printed as well as the exit code (which should be
11 when max-duration has been reached).

### Task list

1. In bin/realize.rs Remove the tokio::time::sleep() +
   process::exit(11) at the beginning of main. To replace it creates a
   tarpc::context::Context with deadline set to Instant::new() +
   cli.max_duration at the beginning. Make max_duration default to 24h.

   - change the code
   - then run "cargo check" to make sure the result compiles, fix any issues

2. Make move_files, move_file and associated functions in src/algo.rs
   again takes a tarpc::context::Context instance and clone it to pass
   it to all the RPCs it makes instead of using
   tarpc::context::current_context.

   - change the code
   - then run "cargo check" to make sure the result compiles, fix any
     issues

3. when counting (success, error) in bin/realize, add a new category:
   interrupted, so the count becomes (success, error, interrupted).
   Interrupted are files that ended with RpcError::DeadlineExceeeded.
   Display interrupted files in the status line (error message:
   "{total_error} file(s) failed, {total_success} file(moved,
   {total_interrupted} interrupted")

   - change the code
   - then run "cargo check" to make sure the result compiles, fix any
     issues

4. just before returning in bin/realize.rs, when deciding whether the
   call was a success, check interrupted count and the context created
   in step 2 to see whether the deadline was exceeded. If deadline was
   exceeded and interrupted count is > 0, print a message to stderr
   with the heading style("INTERRUPTED").for_stderr().red() that says
   "INTERRUPTED {total_success} file(s) moved, {total_interrupted}
   file(s) interrupted" then exit with status 20. Note that if error >
   0, it's still an error not an interruption.

   - change the code
   - then run "cargo check" to make sure the result compiles, fix any
     issues

5. run "cargo test --tests move_files_integration_test
   max_duration_timeout" to run the test for max_duration in
   tests/move_files_integration_test.rs. Make sure it passes with the
   new implementation, fix any issues.

6. Extend the test max_duration_timeout in
   tests/move_files_integration_test.rs to check that the number of
   interrupted files is displayed properly, fix any issues

7. Run "cargo test" and fix any issues including any warnings


## Split into multiple crates {#crates}

To avoid the issue with openssl-sys being dragged into the daemon because
the command-line wants it, do the following:

- split out realize into three crates:
 crate/realize-lib (src/lib.rs and all its dependencies go there)
 crate/realize-daemon (src/bin/realized.rs goes there)
 crate/realize-cmd (src/bin/realize.rs, its dependencies and anything under the push feature goes there)
 and transform the root into a workspace

This way, the command can dependend on openssl-sys and leave it out of
the daemon. No need for the push feature anymore; everything push-related goes into crate/realize-cmd

### Task list

1. Create the three crates and move *all* code into crate/realize-lib.
   Make sure "crate check" and "crate test" succeed, when run in the
   project root dir work (to build the code and run the tests of all
   workspaces), fix any issues.

3. Make crate/realize-cmd depend on crate/realize-lib. Move
   bin/realize.rs to crate/realize-cmd/src/main.rs and
   tests/move_files_integration_test.rs to
   crate/realize-cmd/tests/move_files_integraton_test.rs. Add any
   direct dependencies these files use to the crate's Cargo.toml. Make
   sure "crate check" and "crate test" run in the project root dir
   work, fix any issues.

4. Make crate/realize-daemon depend on crate/realize-lib. Move
   bin/realized.rs to crate/realize-daemon/src/main.rs and
   tests/daemon_integration_test.rs to
   crate/realize-daemon/tests/integraton_test.rs. Add any direct
   dependencies these files use to the crate's Cargo.toml. Make sure
   "crate check" and "crate test" run in the project root dir work.

5. Run "cargo machete" from within crate/realize-lib and remove
   any dependency reported as unnecessary.

6. Remove the "push" feature in crate/realize-lib/Cargo.toml and move
   any code gated by #[cfg(feature = "push")] into crate/realize-cmd.
   Run "crate check" and "crate test" to make sure everything works,
   fix any issues.

7. Run "crate check" and "crate test" in the project dir. Fix any
   issues, including any warnings.

## Addresses {#addr}

Take addresses as string, extract the domain name and pass it to TLS.

## Detect access right errors early in daemon {#daemonaccess}

At startup, the daemon in realized.rs might try to write and delete a
file in the dir it's given, to make sure it has read/write access to
the directory it's given.

Checks:
- directory doesn't exist -> error (fail to start)
- no read access to directory -> error (fail to start)
- no write access to directory -> warning (start)

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

## Close connections {#closeconn}

TLS connections should be closed. Do it properly. Suppress or update
error logs complaining about it; for now they just say "read|write
errored out".

## Implement retries {#retry}

See the section "Error and retries" of spec/design.md.

## Design and add useful logging to realized {#daemonlog}

Think about what should be logged at the error, warning info and debug
level. What format should be followed. What information should be
sent.

## Design and add useful logging to realize {#cmdlog}

Think about what should be logged at the error, warning info and debug
level. What format should be followed. What information should be
sent.

## Optimize reads {#readopt}

Since move_files works on multiple files at once:
 - there's opportunity to write/read for one file while another file computes hash
 - the write and reads are scattered, making it hard on the OS buffers

Experiment with different ways of organizing the work that maybe speed
things up.
