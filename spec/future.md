# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Gate copy by file size {#sizegate}

Instead of allowing one file to copy at a time, allow multiple for a total of up to CHUNK_SIZE bytes. A file > CHUNK_SIZE gets copied one at a time, but smaller files can be grouped together. Might be worth increasing the number of parallel files for that.

## IPV6 + IPV4 {#ipv64}

Localhost is currently resolved to ipv6 address, which isn't what's
expected in the tests, so all tests use 127.0.0.1.

This isn't right; it should be possible to specify localhost (or any
address that resolves to both an ipv6 address and an ipv4 address) and
have it work normally (try ipv6, fallback to ipv4).

This is normally automatic, I expect, but the custom transformation to
SocketAddr screws that up.

## Test retries {#testretries}

Make it possible to intercept RealizeService operations in tests to test:

- write errors later fixed by rsync
- rsync producing a bad chunk, handled as a copy later on

## Drop empty delta {#emptydelta}

Detect empty deltas from the rsync algorithm and skip sending a RPC
for them.

## Drop hash past dest filesize {#sizebeforehash}

Check dest filesize before sending a hash RPC and just store None in
the RangedHash.

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

