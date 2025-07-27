# Consensus - Shifting realities

## Overview

Consensus is the subsystem that manages the content of the
[real](real.md) and the [unreal](unreal.md), sometimes shifting files
from one to the other, realizing or unrealizing them.

This is a job management system, with:

- a dynamic stream of jobs to execute [WorkStream]

- job manager,control and notification  [Churten]

- jobs: [Download], [Realize], [Unrealize]

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

## Churten

Churten reads from the stream of jobs executes them and reports their
result.

Once a job has been executed successfully, the corresponding
DIRTY_TABLE entry is removed. if a job was not executed successfully,
it will be retried later on.

If the DIRTY_TABLE entry that corresponds to a job is replaced (that
is, if the path is modified again), the job can be dropped by Churten,
which then relies on the stream of jobs returning an updated job.

Interface:

- Job execution can be started with `start`

- Job execution can be stopped with `stop`

- Anything that happens is reported as `ChurtenNotification`, that is:

  - a job was created (pending)
  - a job started (running)
  - a job ended (success, failure, abandoned, cancelled)
  - an action within the job started (download, verify, repair, realize, unrealize)
  - progress within an action (byte processed/total bytes). This is relevant mostly for download and repair

- TODO: make the current state, that is the list of active or pending
  jobs, the current action and byte process available upon connection.

This interface is available remotely as a Control Cap'n Proto RPC
accessible through a local UNIX socket. Local processes that belong to
the same user as the daemon (or that are can connect to the stream
according to file mode) can control the daemon through that interface.

This is meant to allow arbitrary UI, starting with a command-line
interface in the crate realize-control.

## Download Jobs

Download jobs are created for file in the cache marked *Keep* that
haven't yet been verified yet.

It does the following:

- Download: download the current version from a remote peer (complete
  the download if some data is already available)

- Verify: once the download is complete, verify its content by hashing
  it and comparing to the hash version

- Repair: if the hash don't match, use rsync to fix the current data

- Verify: once rsync has run through the whole file, verify the
  content a second time and fail the job if it doesn't match

At the end of the job, the cache entry has local_availability *Verified*.

## Realize Jobs

Realize jobs are created for files in the cache marked *Own* that, either:

 - are not in the index yet

 - are in the index, but the version in the index has been replaced by
   a remote peer's (It must be distinguished from the case
   where a remote peer has a different version from what's in the
   index; see below.)

It does the following:

 - Run the Download job until the local cached version is complete and verified

 - Make a hard link from the local cached blob file to the owned file
   (this deletes any current version). This means that the version will be
   reported as a new file to remote peers by the index.

 - Delete the blob, and enable history tracking in the default
   `FileTableEntry` if it's not already enabled and clears history.

History tracking being enabled in a `FileTableEntry` means that it
stores replaced default hashes in the default entry. This allows answering the
question: "has the version in the index been replaced by a remote
peer's" ? The answer is yes if the hash of the indexed version belongs
to the set of previous hashes.

Open issues:

- should the case where making a hard link doesn't work be supported?
This requires copying.

- should the file by reported specially to remote peers?
  [real.md](real.md) calls for it to be reported as "Available", but
  it doesn't seem actually necessary as "Add" would be seen by remote
  peers as "yet another copy" anyways.

## Unrealize Jobs

Unrealize jobs are created for files in the index not marked *Own*
that are available in the cache under the same hash.

It does the following:

 - check that the file is readable and writable by the process user
   and can be moved, if not, abandon the job
 - Create a blob for the cached version, if one doesn't exist yet
 - make a hard line from the file in the index to the blob file.
 - mark the blob file as fully available
 - remove the file in the index. At this point, the index might report
   that the file was removed to other peers. This should be prevented.
   The index must report that the file was dropped locally, so that
   other peers tracking the current peer's version won't think it
   needs to be deleted.

At the end of the process, the file is available in the cache,
complete but unverified.

Open issue:

If the file is modified or opened locally after it was hashed and put
into the index, unrealize should fail and abandon the job, otherwise
local modifications would be lost.

It's ever possible for another process to modify the file after it was
removed from the index root if the file was opened beforehand.

It's very difficult to guarantee that that won't happen. To attempt to do that the following
checks must be run both before and after the job:
- at the beginning of the job, check whether there are any open handle on the file
- then make sure that size and mtime have the expected value for the hash version

If one of these checks fail before the job, fail the job. If one of
these checks fail after the job, move the file back then fail the job.

Even with these protections in place, unrealize remains a dangerous
operation.

How can be "check whether there are any open handle on the file"? `lsof` does that but:
- open files visible to non-root users is limited to proccesses the user owns
- it's not portable

Alternative: the watcher may track opened files, based on the
notification, and provide the information to the job on demand. This
might also not be fully reliable and might be confused by hard links,
but it's worth a try.
