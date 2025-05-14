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

## Add prometeus to realize {#cmdmetrics}

Extend realize with usage metrics. See also the metrics section in
docs/design.md.

Open issue: realize is a command, how should it send metrics to
prometheus for collection? https://last9.io/blog/prometheus-pushgateway/

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

**Proposal:**
- Extend the RPC protocol to support throttle commands, specifying whether limits are per-connection, per-service, or global (document the choice).
- Implement rate limiting in the TCP connection handler using a token bucket or leaky bucket algorithm, ensuring accuracy and low overhead.
- Parse and validate new CLI options in `realize`, including error handling for invalid or conflicting options.
- Ensure limits can be updated at runtime via RPC, and clarify how existing connections are affected (immediate or on next transfer).
- Provide clear error messages and logs for invalid throttle updates or failures.
- Test throttling with unit, integration, and performance tests, including dynamic updates and edge cases (e.g., burst traffic, limit changes).

**Task List:**
1. Design and document the throttle RPC message format, specifying the scope (per-connection, per-service, or global).
2. Implement CLI parsing for throttle options, including validation and error handling.
3. Add rate limiting logic to TCP handlers, ensuring correct behavior under all traffic patterns.
4. Implement RPC handlers to update limits at runtime, and define how updates affect existing connections.
5. Write unit, integration, and performance tests for throttling behavior, CLI parsing, and dynamic updates.
6. Log all throttle changes and errors.
7. Document usage, configuration, and edge cases, including how updates are applied.

