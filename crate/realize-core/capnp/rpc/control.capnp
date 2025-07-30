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

  interface Subscriber {
    notify @0 (notification: ChurtenNotification) -> stream;
  }
}

struct ChurtenNotification {
  arena @0: Text;
  jobId @1: UInt64;

  union {
    new @2: New;
    update @3: Update;
    updateByteCount @4: UpdateByteCount;
    updateAction @5: UpdateAction;
  }

  struct New {
    job @0: Job;
  }
  
  struct Update {
    progress @0: JobProgress;

    # Error Message for failed progress
    message @1: Text;
  }

  struct UpdateByteCount {
    currentBytes @0: UInt64;
    totalBytes @1: UInt64;
  }

  struct UpdateAction {
    action @0: JobAction;
  }
}

struct Job {
  path @0: Text;
  hash @1: Data;
  union {
    download@2 : Download;
    realize@3: Realize;
    unrealize@4: Unrealize;
  }

  struct Download {}
  struct Realize {
    # optional; None if empty
    indexHash @0: Data;
  }
  struct Unrealize {}
}

enum JobType {
  download @0;
  realize @1;
  unrealize @2;
}

enum JobProgress {
  running @0;
  done @1;
  abandoned @2;
  cancelled @3;
  failed @4;
}

enum JobAction {
  download @0;
  verify @1;
  repair @2;
  move @3;
}
