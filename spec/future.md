# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Optimize continuing to copy large files {#rsync}

The sync algorith described in the section "Scenario 1: Move from A to
B" in docs/design.md includes an optimization that's based on fast
rsync.

Let's implement that optimization. The goal is to be able to fix
corrupted files and continue interrupted transfers without having to
send the whole file again.

Task list:

1. Implement the following methods in RealizeServer server.rs by
 calling the corresponding function in the fast_sync module:
 - calculate_signature
 - diff
 - apply_delta

 Define constant for any parameter (such as block size) for later
 evaluation.

 See the section "Service Definition" in docs/design.md, also see the
 documentation of fast_rsync on
 https://docs.rs/fast_rsync/0.2.0/fast_rsync/

 Since the methods just wrap fast_rsync calls, the actual
 definition of what these do is to be found in the documentation
 of fast_rsync.

   - Implement the methods
   - Run "cargo check" to make sure the code compile, fix any issues
   - Add unit tests to verify that these methods work
   - Run "cargo test" to make sure the test run, fix any issues

 2. Extend move_files in src/algo.rs to support the optimization
 described in "Scenario 1: Move from A to B" in docs/design.md that's
 based on fast_rsync.

   - Update the code
   - Run "cargo check" to make sure the code compile
   - Extend the unit tests in src/algo.rs to verify that the optimization
     work. Setup interrupted, corrupted or partially copied files at
     the beginning of the test and make sure that move_files correct all
     these issues.
   - Run "cargo test" to make sure the test run, fix any issues

## Handle SIGTERM {#sigterm}

`realize` and `realized` should handle SIGTERM gracefully and
terminate immediately.

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

## Implement Max-duration in the realize command {#max-duration}

As described in the design doc.
