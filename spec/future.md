# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## Use channels for progress {#newprogress}

Implementing Progress as callback as move_files() does in
crate/realize-lib/src/algo.rs isn't quite correct in an async
scenario.

The problem is that to be Sync + Send, the progress callback must
block, using a blocking, non-tokio mutex, which you're not supposed to
do outside of spawn_blocking.

Let's fix that by switching to an channel-based progress: instead of
calling a callback, move_files send events to a channel.


### Task list

1. Introduce a ProgressEvent enum in crate/realize-lib/src/algo.rs to
   replace the Progress trait, but don't use it yet.

   Add DirectoryId to all events. Events in FileProgress all have the
   DirectoryId + path as extra argument so that:

```rust
  pub trait Progress: Sync + Send {
    fn set_length(&mut self, total_files: usize, total_bytes: u64);

    fn for_file(&self, path: &std::path::Path, bytes: u64) -> Box<dyn FileProgress>;
  }

  pub trait FileProgress: Sync + Send {
    fn verifying(&mut self);
    fn rsyncing(&mut self);
    ...
```

   becomes (pseudo code)

```

  enum ProgressEvent {
    MovingDir{dir_id: DirectoryId, total_files: usize, total_bytes: u64},
    MovingFile{dir_id: DirectorId, path: PathBuf, bytes: u64},
    VerifyingFile{dir_id: DirectoryId, path: PathBuf},
    SyncingFile{dir_id: DirectoryId, path: PathBuf},
    ... the same for all methods in FileProgress
  }

```

  *Important* Step 1 isn't finished until you've run "cargo check" and
  the code compiles. Fix any issues these report.

2. Rewrite move_files() to take a channel, renaming it
   move_files_with_channel() and add an adapter to keep existing code
   running that might look like the following:

```
pub async fn move_files<T, U, P>(
    ctx: tarpc::context::Context,
    src: &RealizeServiceClient<T>,
    dst: &RealizeServiceClient<U>,
    dir_id: DirectoryId,
    progress: &mut P,
) -> Result<(usize, usize, usize), MoveFileError> {
   let (tx, rx) = tokio::sync::mpsc::Sender<ProgressEvent>;
   let progress_dir_id = dir_id.clone();
   let adapter = tokio::spawn(async move { progress_adapter(rx, progress_dir_id, progress) });
   let res = move_files_with_channel(ctx, src, dst, dir_id, Some(tx)).await;
   let _ = adapter.abort();

   res
}

// write progress_adapter()

```

 *Important* Step 2 isn't finished until you've run "cargo check" then
 "cargo test", the code compiles and all tests pass. Fix any issues these report.

3. Rewrite all tests in crate/realize-lib/src/algo.rs call
   move_files_with_channel() directly. Adapt progress_trait_is_called
   to test based on the events and remove MockProgress.

 *Important* Step 3 isn't finished until you've run "cargo check" then
 "cargo test", the code compiles and all tests pass. Fix any issues these report.

4. Rewrite all other tests that use move_files to use
   move_files_with_channel.

 *Important* Step 4 isn't finished until you've run "cargo check" then
 "cargo test", the code compiles and all tests pass. Fix any issues these report.

5. Rewrite the code in crate/realize-cmd/src/main.rs that calls
  move_files to call move_files_with_channel instead. Make the
  smallest change possible that keep things running. You might move
  progress_adapter to crate/realize-cmd/src/progress.rs if that's
  convenient.

 *Important* Step 5 isn't finished until you've run "cargo check" then
 "cargo test", the code compiles and all tests pass. Fix any issues these report.

6. Clean up crate/realize-cmd/src/progress.rs to make it
   channel-centric.

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

