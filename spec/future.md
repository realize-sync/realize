# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Reduce visibility {#visibility}

1. Turn all types marked "pub" in crate/realize-lib/src into
   "pub(crate)".

2. Run "cargo check" and make "pub" again everything that is needed to
   compile the other crates.

3. Run "cargo check --tests" and list everything that is needed for
   the tests to compile, to decide on a case-by-case basis if it
   should be exposed.

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
