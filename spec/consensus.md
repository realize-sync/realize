# Consensus - Shifting realities

## Overview

Consensus is the subsystem that manages the content of the
[real](real.md) and the [unreal](unreal.md), sometimes shifting files
from one to the other, realizing or unrealizing them.

This is a job management system, with:

- a dynamic stream of jobs to execute [WorkStream]

- a certain number running jobs  [Workers]

- controls for starting and stopping jobs [Interface]

- feedback channel that reports job execution status and progress [Progress]

Arenas, files and directories are marked with:
 - *Watch* keep the file in the cache, download it when needed
 - *Keep* keep the file in the cache, download it whenever it is updated
 - *Own* keep the file in the index, update it if peers modify it
which describes what we want to do with the files.

Marks are hierarchical: To figure out the mark of a file, look for a
mark set on the file, on any of its parent, on the arena and default
to Watch.

## WorkStream

This is a `Stream<Job>`. Calls to next consume one or more
`Stream<Path>` of dirty paths and create one `Job` which checks the
index and cache and does whatever it needs to do.

### Stream<Path>

`Stream<Path>` is created by marking dirty files that are modified in
the cache (only the default version matters), the index, or that have
their mark changed.

This is implemented as a dirty table on the database shared by an
arena's index and cache which is filled by `UnrealIndexBlocking`
`ArenaCache` and `PathMarker` whenever they change something.

The dirty table is meant to work as a set with entries ordered by
last modification time.

Tables:
- **DIRTY_TABLE** Key: `&str (Path)` Value: `u32` (DIRTY_LOG_TABLE key)
- **DIRTY_LOG_TABLE** Key: `u32` (counter, increasing) Value: `&str` (Path)

Whenever a file is marked dirty, it is added in both table with
`DIRTY_LOG_TABLE.last()+1` as u32 key. If writing overwrites an entry
in DIRTY_TABLE, take its value and delete the corresponding entry from
the DIRTY_LOG_TABLE as well. When clearing an entry, clear it in both
tables. After removing everything from DIRTY_TABLE, leave the counter
in DIRTY_LOG_TABLE associated with "". This gives a chance to the app
to clear the counter, when it's ready to do so.

With that, it's possible for `Stream<Path>` to iterate through
DIRTY_LOG_TABLE in order, with small transactions, remembering the
counter to continue reading where it left off.

Paths that are modified more than once while going through the stream
might be reported more than once (A). Only the last modification of
paths that are modified more than once when no stream is active is
reported (B). A is convenient, because it allows checking running jobs
to see if they're still relevant. B is convenient, because only the
last state of a path matters.

A watcher (tokio watch channel) reports counter changes so listeners
can query the dirty table for the latest changes.

### Stream<Job>

`Stream<Job>` reads `Stream<Path>` and decides what to do for each
paths, depending on what it finds in the cache, index, and the path
mark, an action that is represented by a Job.

Once a Job ends, it deletes the dirty mark on the path if the counter
is the counter that is kept by the Job (This makes sure the path
didn't change while the job was being executed.)

Note that in many cases, there's just nothing to do and the dirty mark
can just be removed right away.

Improvement: Upon getting a Path that corresponds to a running Job,
let the job know about the path and decide to either:
- give up
- decide that that modification changes nothing and increase the job
  Path counter

Jobs:
 - *Download* Download a file from another peer, then verify the local content
 - *Realize* Move a blob from the cache to the real
 - *Unrealize* Move a file from the real to the cache, as a blob

Realize and Unrealize are normally quick, as the file change should
just a move, as arena root and cache blob store directory should be on
the same filesystem (a slower fallback to copy might or might not be
supported; the issues is that, if interrupted, we can end up with
partial copies that need to be cleaned up.)

Download can be slow, depending on the size of the file and the speed
of the connection to the peer.

Jobs can fail. Failed jobs should be retried after other tasks are
done and after some delay. (TODO: Detail how)

Jobs can be stopped at any time, either gracefully or due to
the process going down. The Job should be able to continue where they
left off, as long as it is still necessary.

For example, a *Download* should update the progress in the database
periodically (after flushing and syncing cache content), as checkpoint
for if the process goes down.

## Workers

Workers continuously pick up jobs from Stream<Job> and execute them.
There's an arbitrary number of workers (or job slots).

## Interface and Progress

TBD

It should be possible to see:
- jobs that have been executed (with expiration) and their result
- jobs that are currently executing, their progress and status
- a small number jobs that will soon be executed
and get updates whenever anything changes.

The result should be very close to the current terminal-based progress
output of [movedirs](movedirs.md)
