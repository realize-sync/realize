# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Design and add useful logging to realized {#daemonlog}

Think about what should be logged at the error, warning info and debug
level. What format should be followed. What information should be
sent.

## Design and add useful logging to realize {#cmdlog}

Think about what should be logged at the error, warning info and debug
level. What format should be followed. What information should be
sent.

## Throttle a TCP connection {#throttle}

The bytes sent per second in a specific TCP connection should be
limited. This should be done by making a RPC call that sets the upper
limit in bytes per second for the bytes they *send*.

The limits are set by the `realize` command on both RPC services and
specified as download and upload limits on the command-line:
--throttle 1M to throttle both to 1M/s, --throttle-up 1M
--throttle-down 512k to throttle upload to 1M and download to
512k/s.

### Computing the rate limit (DONE)

Add async_speed_limit as a dependency for tracking the limits:
https://docs.rs/async-speed-limit/0.4.2/async_speed_limit/index.html

### Applying the rate limit to a tokio Stream (DONE)

Apply the rate limit on Stream, for correctness, by writing and the
using RateLimitStream combinator.

Only the writes should be limited and delayed.

To get started, see this Stream combinator:
  https://docs.rs/governor/latest/governor/state/direct/struct.RatelimitedStream.html
However:
  - this limits the reads, but we want to rate-limit writes (AsyncWrite)
  - this is a combinator for std Stream, and TcpStream is a tokio Stream

For TCP, the stream will be a TCP stream, See RunningServer::bind.

PROBLEM: In-process uses a Channel, not a Stream so rate-limiting
would only be for TCP connections, no in-process ones.

### Setting the rate limit

Rate limiting should be configurable an a RPC service by calling the
new service method RealizeService.configure(config: Config).

The new Config type contains an optional write rate-limit that can be
set. Setting the config sets the rate-limit on the RateLimitStream
combinator, so RealizeServer needs to have access to that or
RealizeServer and RateLimitStream should share an object that
RealizeServer can modify.

When given a write limit, the `realize` command sets the limit on the
dst RPC service. When given a read limit, the `realize` command sets
the limit on the src RPC service. (Setting the rate-limit config only
has effects on TCP services, on in-process service it won't have any
effect. Let's ignore that for now.)

### Task List

1. (DONE) Write a RateLimitStream combinator, unit-test it thoroughly.

2. Add RealizeService.configure as described above, leave the
   implementation in RealizeServer a no-op. Write thorough unit tests,
   make sure all tests pass, fix any issues.

3. Create RateLimitStream in RunningServer::bind src/transport/tcp.rs
   and pass the limiter to RealizeServer so RealizeServer.configure
   can call Limiter::set_limit to set a different rate limit. Write
   unit tests, make sure all tests pass, fix any issues.

4. Extend the `realize` command in src/bin/realize.rs to call
   RealizeService.configure as configured by the command-line
   arguments throttle_up/down in bytes-per-second. Make sure
   everything compiles, fix any issues.

5. Add an integration test that sets these and checks for a log
   entry (log::info!) that says that rate-limiting has and what it is
   deep in the code. Make sure the test passes, fix any issues.

6. Change throttle_up/down command-line argument type to a type that
   accepts shortcuts like, for example, 1K or 1M for 1024 and
   1024*1024 bytes, respectively. Use that instead of a raw number.

   Possibly parse_size can help:
   https://docs.rs/parse-size/latest/parse_size/

   Extend the integration tests to use such units, make sure the test
   passes, fix any issues.

## Optimize reads {#readopt}

Since move_files works on multiple files at once:
 - there's opportunity to write/read for one file while another file computes hash
 - the write and reads are scattered, making it hard on the OS buffers

Experiment with different ways of organizing the work that maybe speed
things up.
