# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Handle SIGTERM {#sigterm}

`realize` and `realized` should handle SIGTERM gracefully and
terminate immediately.

**Proposal:**
- Use Rust's signal handling libraries (e.g., `signal-hook`) to listen for SIGTERM in both `realize` and `realized` processes.
- On receiving SIGTERM, ensure all resources are cleaned up, including closing open files, network sockets, and terminating child processes.
- Handle SIGTERM in all operational states (idle, processing, waiting for I/O, etc.).
- Ensure child processes are terminated safely, using process groups or explicit tracking, and handle any errors during termination.
- Log the receipt of SIGTERM and the shutdown sequence for debugging and audit purposes.
- If cleanup fails, log the error and exit with a non-zero status code.

**Task List:**
1. Add `signal-hook` as a dependency if not present.
2. Register a SIGTERM handler in both binaries.
3. Implement cleanup logic: close files, sockets, and terminate child processes, ensuring all are handled even if errors occur.
4. Ensure the handler logs the event, the cleanup steps, and exits with the correct code (0 for clean, non-zero for errors).
5. Test SIGTERM handling in all operational states (idle, busy, waiting, etc.).
6. Write integration tests to send SIGTERM and verify graceful shutdown and resource cleanup.
7. Document the behavior, including edge cases, in the README and code comments.

## Handle SIGHUP {#sighup}

`realized` should handle SIGHUP by re-loading the config file.

**Proposal:**
- Use `signal-hook` to listen for SIGHUP in the `realized` process.
- On SIGHUP, reload the configuration file and apply changes without restarting the process.
- Clearly define which configuration fields are reloadable and which require a restart.
- Ensure thread safety and synchronization when applying new configuration.
- If reload fails, log the error, retain the previous configuration, and do not apply partial changes.
- Log the reload event, including success or failure, and the reason for any failure.

**Task List:**
1. Register a SIGHUP handler in `realized`.
2. Implement config reload logic, specifying which fields can be hot-reloaded and which cannot.
3. Ensure thread safety and atomicity when updating configuration.
4. On reload failure, log the error and keep the previous configuration active.
5. Log all reload attempts and outcomes, including reasons for failure.
6. Add tests to send SIGHUP and verify config reload, including error scenarios and partial reload attempts.
7. Update documentation to describe SIGHUP behavior, reloadable fields, and error handling.

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

## Implement --max-duration in the realize command {#max-duration}

As described in the design doc.

**Proposal:**
- Add a `--max-duration` CLI option to `realize` to specify a maximum allowed runtime (wall-clock time, not CPU time).
- Use a timer to track elapsed time and terminate the process (and any children) when the limit is reached.
- On timeout, gracefully terminate all processes, clean up resources, and log the event.
- Specify what happens to ongoing work and child processes (e.g., terminate immediately, allow for short grace period, etc.).
- Ensure a clear exit code and log message when the duration is exceeded.
- Handle partial results and cleanup on timeout.

**Task List:**
1. Add CLI parsing for `--max-duration`, validating input and providing clear error messages.
2. Implement a timer or async task to track wall-clock runtime.
3. On timeout, gracefully terminate all processes, clean up resources, and log the event, including what was interrupted.
4. Decide and document whether to allow a grace period for cleanup or terminate immediately.
5. Handle partial results and ensure no resource leaks on timeout.
6. Add tests to verify correct timeout behavior, including edge cases (e.g., timeout during heavy load, idle, or shutdown).
7. Update documentation to describe the new option, shutdown semantics, and edge cases.

## Add monitoring with prometheus to realized {#daemonmetrics}

Extend realized with server and usage metrics. See also the metrics
section in docs/design.md.

## Add monitoring with prometeus to realize {#cmdmetrics}

Extend realize with usage metrics. See also the metrics section in
docs/design.md.

Open issue: realize is a command, how should it send metrics to
prometheus for collection?

## Design and add useful logging to realized {#daemonlog}

Think about what should be logged at the error, warning info and debug
level. What format should be followed. What information should be
sent.

## Design and add useful logging to realize {#cmdlog}

Think about what should be logged at the error, warning info and debug
level. What format should be followed. What information should be
sent.
