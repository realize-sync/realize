# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Deadlines {#deadlines}

Set an overall deadline for realize (24h) set by --max-duration, have reconnect handle deadlines.

Reconnect should:
 - stop reconnecting on overall deadline exceeded (before waiting for backoff, if now+backoff >= deadline)
 - limit individual deadline to some small value (1m), make sure small deadline < overall deadline
 - retry requests with deadline exceeded if overall deadline is not exceeded

non-tcp client without reconnect just have one large deadline.

move_files again takes one context and pass it to all the RPCs it
makes instead of using current_context.

WithDuration can be removed.

Goal: max-duration is now implemented using deadline. No need to kill
the process when max-duration is exceeded (test! allow some extra time
for shutdown in the test)

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
