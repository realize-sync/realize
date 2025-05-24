# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Command Output {#cmdoutput}

Change argument to control log output in realize-cmd, instead of just --quiet:

--output=quiet : error only
--output=progress : progress + errors [default]
--output=log : log with custom RUST_LOG if unset, errors are logged and not printed

Implementation wise, --output=log disabling errors means that
everything that's output to stderr should also be sent to the log at
warning or error level.

### Task list

1. Replace --quiet with --output argument, an enum-type argument,
   which default to --output=progress.

2. Everywhere quiet is used, use that new enum type.

3. When --output=log and RUST_LOG is unset, set it to "warn,realize_cmd=info,realize_cmd::progress=info"

4. Find all the places in crate/realize-cmd/src/main.rs and crate/realize-cmd/src/progress.rs where some information is printed only to stdout or stderr and

  - add it to the log in addition at info (for stdout) warn or error
    level (for stderr), do that always, regardless of --output value.
    Don't attempt to print it nicely with index ([n/M]) colors or
    alignment {:<10}, make it just a plain message.

  - don't print to stdout or stderr if --output=log, print only to
    stderr if --output=quiet (effect of the current boolean quiet)

5. Run "cargo check" to make sure everything compiles, fix any issues.

6. Update the tests in
   crate/realize-cmd/tests/move_files_integration_test.rs that set or
   test --quiet to use --output.

7. Add a test for --output=log that checks that :
   - messages are not printed
   - errors are only logged, not printed anymore
   - the events MovingDir, MovingFile, CopyingFile, FileSuccess and FileError produce a log entry (and what that entry is)
   - the summary is logged, not printed

8. Run "cargo test --tests move_files_integration_test" to make sure the relevant tests pass, fix any issues.

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

## Optimize reads {#readopt}

Since move_files works on multiple files at once:
 - there's opportunity to write/read for one file while another file computes hash
 - the write and reads are scattered, making it hard on the OS buffers

Experiment with different ways of organizing the work that maybe speed
things up.

