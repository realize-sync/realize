# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Add prometheus to realized {#daemonmetrics}

Have daemon export metrics.

1. Export metrics at http://host:port/metrics

- Add a command line argument --metrics-addr to realized that takes a
  host:port. This is the address where the prometheus metrics should
  be exported.

- Add dependency rouille 3.6

- When this argument is set, start a mini web-server with tokio::spawn
  using the rouille package. The server should return 401 for
  everything but the request path "/metrics"

- When the request path is "/metrics" return "{}" for now

- Add the code

- Run "cargo check" to verify the code, fix any issues

- Add a test case to src/daemon_integration_test.rs that starts
  a server and access "http://host:port/metrics"

2. Export prometheus metrics

- Add dependency on prometheus 0.14 with "process" feature

- When the request path is "/metrics", call prometheus::gather(),
  encode the result with prometheus::TextEncoder and output it.

- Add the code

- Run "cargo check" to verify the code, fix any issues

- Extend the prometheus test case in src/daemon_integration_test.rs
  to test the output of "http://host:port/metrics" This should
  include process metrics, so shouldn't be empty.

## Add prometheus to realize {#cmdmetrics}

Extend realize with usage metrics. See also the metrics section in
docs/design.md.

Since realize isn't meant to always be up, it makes sense for it to
support pushing its final metrics, so we can see it's run even if it's
for less than a minute. See example:
https://raw.githubusercontent.com/tikv/rust-prometheus/refs/heads/master/examples/example_push.rs

Since realize is expected to run for hours in some cases, it makes
sense for it to support pull metrics, though the prometheus server
must be configured to be ok with not finding anything.

`realize` should support both, turned on by --metrics-addr (like
realized, address to bind to) and --metrics-pushgateway (gateway
address).

Code for handling metrics should be put into src/metrics.rs, so the
commands can just call one or two functions. Code for pull metric
that's currently in realized should be moved to src/metrics.rs so it
can be called from realize as well.

Task list

1. Move export_metrics from src/bin/realized.rs to src/metrics.rs (do
   remember to register the new metrics module in src/lib.rs)

   Run "cargo check" to make sure everything still compiles, fix
   any issues.

2. Add the command-line argument --metrics-addr to src/bin/realize.rs,
   call export_metrics. Also add METRIC_UP to src/bin/realize.rs,
   (realize_command_up) which is just incremented to 1 just before
   calling move_files.

   Run "cargo check" to make sure everything still compiles, fix
   any issues.

3. Extend the integration test move_files_integration_test.rs to
   add a test that checks that the metrics are exposed. Just like
   for the ctrlc test, it might be a good idea to point to to an
   address that will never return then kill it, so the command won't
   return before we could fetch the /metrics URL.

   Run "cargo test --tests move_files_integration_test" and make sure
   everything passes. Fix any issues.

4. Add the command-line argument --metrics-pushgateway to
   src/bin/realize.rs --metrics-job (defaults to realize) and
   --metrics-instance (no defaults) Put most of the code for that into
   src/metrics.rs, so realize can just call a function at the end to
   push the metrics.

   No need to support authentication; it should be a simple HTTP
   request. The job id passed to push (instead of "example_push")
   should code from the command-line argument --metrics-job and there
   should be an instance label if --metrics-instance is set.

   Run "cargo check" to make sure everything still compiles, fix
   any issues.

5. Add an integration test into tests/move_files_integration_test.rs
   for metrics gateway.

   For that, start a fake gateway in a tokio::spawn defined using
   rouille that accepts a single POST at the expected URL
   (http://gatewayaddress/metrics/job/<job-id>) then make the data
   available to the main thread, using a oneshot channel, for example.

   Point the command to that gateway and check that the data was
   posted as expected.

   Run "cargo test --tests move_files_integration_test" and make
   sure everything passes. Fix any issues.

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

### Computing the rate limit

Use governor for tracking the limits:
https://docs.rs/governor/latest/governor/ See also user guide on
https://docs.rs/governor/latest/governor/_guide/index.html

### Applying the rate limit to a tokio Stream

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
would only be for remote connections.

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

1. Write a RateLimitStream combinator, unit-test it thoroughly.

2. Add RealizeService.configure as described above, leave the
   implementation in RealizeServer a no-op. Write thorough unit tests,
   make sure all tests pass, fix any issues.

3. Apply that combinator in RunningServer::bind src/transport/tcp.rs
   and bind it to RealizeServer so RealizeServer.configure can set
   the write limit. Write thorough unit tests, make sure all tests
   pass, fix any issues.

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
