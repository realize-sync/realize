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
  job @1: Job;

  union {
    update @2: Update;
    updateByteCount @3: UpdateByteCount;
    updateAction @4: UpdateAction;
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
  # counter intentionally left out

  # Once there is more than one job type:
  # union {
    download @1: Download;
  # }

  struct Download {
    hash @0: Data;
  }
}

enum JobProgress {
  pending @0;
  running @1;
  done @2;
  abandoned @3;
  cancelled @4;
  failed @5;
}

enum JobAction {
  download @0;
  verify @1;
  repair @2;
}