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

## Addresses {#addr}

Take addresses as string, extract the domain name and pass it to TLS.

Create a HostPort struct in the crate realize-lib, in
src/transport/tcp.rs that can be used as follows:

```rust
 let hostport = HostPort::parse("localhost:1234").await?;
 assert_eq!(1234, hostport.port());
 assert_eq!("localhost", hostport.host());
 assert_eq!(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 1234), hostport.addr());

 let hostport = HostPort::parse("example.com:8888").await?;
 assert_eq!(8888, hostport.port());
 assert_eq!("example.com", hostport.host());

 assert_eq!(HostPort::parse("myhost").await.is_err());
 assert_eq!(HostPort::parse("doesnotexist:1000").await.is_err());

 // Also supports IPV6 addresses made up of numbers and colons, here ::1 for IPV6 localhost.
 let hostport = HostPort::parse("[::1]:8000").await?;
 assert_eq!("::1", hostport.host());
 assert_eq!(8000, hostport.port());

 // A HostPort can be created from a SocketAddr, using HostPort::from(SocketAddr) (implements the trait From<SocketAddr>)
 assert_eq(HostPort::parse("127.0.0.1:8000").await?, HostPort::from(HostPort::parse("127.0.0.1:8000").await?.addr()))
```

HostPort defines Debug and Clone, Eq, and Hash. It has a nice Display
representation that's "{host}:{port}".

HostPort::parse() fails if it cannot resolve the DNS address. It uses
tokio::net::lookup_host to resolve hostnames.

### Task list

1. Define HostPort in in the crate realize-lib, in src/transport/tcp.rs
   and add unit tests

   - write the code as described above
   - run "cargo check" to make sure it builds, "cargo test --lib" to make sure the unit tests pass, fix any issues including any warnings, even the minor ones

2. Update connect_client() and start_server() to take a HostPort instead of a SocktAddr or a ToSocketAddrs.
   - update the code
   - fix any tests that call the methods
   - run "cargo check" to make sure it builds, "cargo test --lib" to make sure the unit tests pass, fix any issues including any warnings, even the minor ones


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
