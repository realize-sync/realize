# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Split into multiple crates {#crates}

To avoid the issue with openssl-sys being dragged into the daemon because
the command-line wants it, do the following:

- Read about cargo workspaces on
   https://doc.rust-lang.org/book/ch14-03-cargo-workspaces.html

- Transform the current project into a workspace. Split out realize
 into three crates: crate/realize-lib (src/lib.rs and all its
 dependencies go there) crate/realize-daemon (src/bin/realized.rs goes
 there) crate/realize-cmd (src/bin/realize.rs, its dependencies and
 anything under the push feature goes there) and transform the root
 into a workspace

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
