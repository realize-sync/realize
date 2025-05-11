# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Handle SIGTERM {#sigterm}

`realize` and `realized` should handle SIGTERM gracefully and
terminate immediately.

Test it in an integration test. Fix anything that needs fixing.

## Handle SIGHUP {#sighup}

`realized` should handle SIGHUP by re-loading the config file.

## Throttle a TCP connection {#throttle}

The bytes sent per second in a specific TCP connection should be
limited. This should be done by making a RPC call that sets the upper
limit in bytes per second for the bytes they *send*.

The limits are set by the `realize` command on both RPC services and
specified as download and upload limits on the command-line:
--throttle 1M to throttle both to 1M/s, --throttle-up 1M
--throttle-down 512k to throttle upload to 1M/s and download to
512k/s.

## Implement --max-duration in the realize command {#max-duration}

As described in the design doc.
