# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Error handling in move_files {#moverrors}

Task list

1. DONE In src/algo.rs, move_files should count and return:
  - the number of files that were successfully processed by move_file
  - the number of files that were unsuccessfully processed by move_file

  Instead of forwarding the error, it should log that error with
  log::error!. The log message includes the directory id and path
  of the files that were unsuccessfully moved.

- Update the code

- Run "cargo check" to make sure it still compiles, fix any issues, including any warnings

- Add a new test case to cover the case of a partially successful
  move_files. One way to cause an error one one file in a test would
  be to make that file not readable.


2. Have move_file and move_files in src/algo.rs return a custom error
   type instead of anyhow::Error. That error type should be an enum
   type MoveFileErrors created with thiserror that covers all type of
   error found while executing move_file and move_files. There should
   at least be one enum for tarpc::RpcError and one for RealizeError.

- Update the code

- Run "cargo check" to make sure it still compiles, fix any issues,
  including any warnings

## Progress in the realize command {#progress1}

   Write a trait for forwarding progress information from move_files,
   defined in src/algo.rs, call it Progress.

   The Progress trait should look like this (pseudo-code):

   ```
   // Progress instance passed to move_files()
   trait Progress {
     // Once the number of files to move is known, report it
     fn set_length(&mut self, total_files, total_bytes)

     // Process a file
     fn for_file(&mut self, path, bytes) -> FileProgress

     // There was an error not tied to a file
     fn error(&mut self, err: FileMoveError)
   }

   // File-specific progress instance passed to move_file()
   trait FileProgress {
     // Checking the hash at the beginning or end
     fn veryfying(&mut self)

     // Transferring data
     fn moving(&mut self)

     // Increment byte count for the file and overally byte
     // count by this amount.
     fn inc(&mut self, bytecount)

     // File was moved, finished and deleted on the source.
     // File is done, increment file count by 1.
     fn success(&mut self)

     // Moving the file failed.
     // File is done, increment file count by 1.
     fn error(&mut self, err: FileMoveError)
   }
   ```

   There should be one empty implementation of that trait, called
   NoProgress that can be used everywhere move_files doesn't support
   progress, that is, everywhere is called from currently.

  Task list:

   - Write the trait and its empty implementation
   - Run "cargo check" to make sure it compiles, fix any errors
   - Call the trait from move_files
   - Run "cargo check" to make sure it compiles, fix any errors
   - Add one ore more unit tests that makes sure progress information is
     called correctly in move_files
   - Run "cargo test <testname>" to make sure the test passes, fix any errors
   - Run "cargo test" to make sure everything all tests pass, fix any errors

## Progress in the realize command {#progress2}

  Implement the Progress trait defined in src/algo.rs in
  src/bin/realize.rs.

  The realize command should display progress information to stdout
  using the package indicatif. It should report status messages to
  stdout using println! and error messages to stderr using eprintln!.

  Messages should follow the format:

  ```
    [{file}/{total file}] {prefix} {directory-id}/{path} {progress}: {msg}
  ```

  {file} is the current file index, 1-based and {total file} is the
  total number of files to process. So [1/10] is the first file
  processed out of 10.

  {prefix} describes what is happening or has happened
   - Copying
   - Hashing
   - Deleting
   - Checking
   - ERROR
   - Moved
  It has a fixed width of 9 chars, so the paths align on the messages
  that are output.

  {directory-id}/{path} is the directory id and path as it appears in
  arguments to RealizeService.

  {progress} is a progress bar, such as [==>   ]

  {msg} gives more information on what happens, usually an error message
  when {prefix} is ERROR.

  Not all component need to be there. File count is only there when
  processing files. ": {msg}" is only there for errors

  For example:

  ```
    [1/2] Copying  dirid/subdir/foo.txt [====>                  ]
    [1/2] Deleting dirid/subdir/foo.txt
    [1/2] ERROR   dirid/subdir/foo.txt: File not found
    [3/5] Moved   dirid/subdir/foo.txt
    Checking dirid [======>              ]
    ERROR dirid: network connection failed
  ```

  File progress messages (Copying, Deleting, Moving, Hashing, Checking)
  should look like the following (ProgressStyle template):

    [{pos}/{len}] {prefix:<9.cyan.bold} [{bar:57.cyan/blue}]

  with progress_chars "=> "

  Success reports (Moved) should be written to stdout with the
  progress suspended. Moved takes 9 chars and is written in bold green
  (console::style("Moved").for_stdout().bold().green()).

  Error reports (ERROR) should be written to stderr with the progress
  suspended. ERROR takes 9 chars and is written in bold red
  (console::style("ERROR").for_stderr().bold().red()).

  For an example, check out the following implementation, which does
  something very much like this:

  /Users/stephane/projects/hoarder/src/commands/output.rs

  Task list:

  - Implement the Progress trait using indicatif in src/realize.rs as described
  - Run "cargo check" and fix any issues, including any warnings
  - Write an integration test that verifies that stdout at the end
    of the command is as it should be.
  - Run "cargo test" and fix any issues

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

