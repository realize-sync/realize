@0xe3dce7cd32acae48;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("rpc::control");

# Control the local realize server.
interface Control {
  churten @0 () -> (churten: Churten);

  setMark @1 (req: SetMarkRequest) -> ();
  setArenaMark @2 (req: SetArenaMarkRequest) -> ();
  getMark @3 (req: GetMarkRequest) -> (res: GetMarkResponse);
}

struct SetMarkRequest {
  arena @0: Text;
  path @1: Text;
  mark @2: Mark;
}

struct SetArenaMarkRequest {
  arena @0: Text;
  mark @1: Mark;
}

struct GetMarkRequest {
  arena @0: Text;
  path @1: Text;
}

struct GetMarkResponse {
  mark @0: Mark;
}

enum Mark {
  watch @0;
  keep @1;
  own @2;
}

interface Churten {
  subscribe @0 (subscriber: Subscriber) -> ();
  start @1 () -> ();
  shutdown @2 () -> ();
  isRunning @3 () -> (running: Bool);
  recentJobs @4 () -> (res: List(JobInfo));

  interface Subscriber {
    notify @0 (notification: ChurtenNotification) -> stream;

    # Provide a list of active jobs as catchup and as a
    # way to reset the stream when the channel is full.
    #
    # Whenever sent, this should overwrite the state
    # built from previous notifications. Jobs not reported
    # should be considered finished (with unknown result).
    reset @1 (jobs: List(JobInfo)) -> stream;
  }
}

struct ChurtenNotification {
  arena @0: Text;
  jobId @1: UInt64;

  union {
    new @2: New;
    start @3: Start;
    finish @4: Finish;
    updateByteCount @5: UpdateByteCount;
    updateAction @6: UpdateAction;
  }

  struct New {
    job @0: Job;
  }

  struct Start {}
  
  struct Finish {
    progress @0: JobProgress;
  }

  struct UpdateByteCount {
    currentBytes @0: UInt64;
    totalBytes @1: UInt64;
    index @2: UInt32;
  }

  struct UpdateAction {
    action @0: JobAction;
    index @1: UInt32;
  }
}

struct Job {
  path @0: Text;
  hash @1: Data;
  union {
    download@2 : Download;
    realize@3: Realize;
  }

  struct Download {}
  struct Realize {
    # optional; None if empty
    indexHash @0: Data;
  }
}

struct JobInfo {
  arena @0: Text;
  id @1: UInt64;
  job @2: Job;
  progress @3: JobProgress;
  action @4: JobAction;
  byteProgress @5: ByteProgress;
  notificationIndex @6: UInt32;
}

struct ByteProgress {
  current @0: UInt64;
  total @1: UInt64;
}

enum JobType {
  download @0;
  realize @1;
  unrealize @2;
}

struct JobProgress {
  type @0: Type;
  message @1: Text;

  enum Type {
    pending @0;
    running @1;
    done @2;
    abandoned @3;
    cancelled @4;
    failed @5;
    noPeers @6;
  }
}

enum JobAction {
  none @0;
  download @1;
  verify @2;
  repair @3;
  move @4;
}
