@0xe3dce7cd32acae48;

using Rust = import "/capnpc/rust.capnp";
using import "result.capnp".Result;

$Rust.parentModule("rpc::control");

# Control the local realize server.
interface Control {
  churten @0 () -> (churten: Churten);
  
  listPeers @1 () -> (res:List(PeerConnectionInfo));
  # list peers and their connection status
  
  keepConnected @2 (peer: Text) -> ();
  # attempt to keep connected to the given peer
  
  disconnect @3 (peer: Text) -> ();
  # disconnect to the given peer or stop trying to connect

  createArena @4 (req: CreateArenaRequest) -> (res: Result(CreateArenaResponse, CreateArenaError));
  # create a new local arena

  removeArena @5 (req: RemoveArenaRequest) -> (res: Result(RemoveArenaResponse, RemoveArenaError));

  listAttr @6 (req: ListAttrRequest) -> (res: Result(ListAttrResponse, AttrError));
  getAttr @7 (req: GetAttrRequest) -> (res: Result(GetAttrResponse, AttrError));
  setAttr @8 (req: SetAttrRequest) -> (res: Result(SetAttrResponse, AttrError));

  emptyTrash @9 (arena: Text) -> ();
  emptyCache @10 (arena: Text) -> ();
}

struct PeerConnectionInfo {
  peer @0:Text;
  connected @1:Bool; # if ture, peer is currently connected
  keepConnected @2:Bool; # if true, the server will keep trying to connect
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
}

struct CreateArenaRequest {
  arena @0: Text; # name of the arena to create
  dir @1: Data; # path to the arena's local directory 
}

struct CreateArenaResponse {}

struct CreateArenaError {
  
  issue @0: SanityCheckIssue;
}

struct RemoveArenaRequest {
  arena @0: Text; # name of the arena to remove

}

struct RemoveArenaResponse {
  dir @0: Data; # path to the arena's local directory, may be missing
  workdir @1: Data; # path to the arena's local directory, may be missing
}

struct RemoveArenaError {
}

struct SanityCheckIssue {
  check @0: Check;
  path @1: Data;

  enum Check {
    exists @0;
    readableDir @1;
    writableDir @2;
    sameDevice @3;
  }
}

struct AttrError {
  union {
    noSuchAttribute @0: Void;
    invalidAttributeValue @1: Void;
    unknownArena @2: Void;
    pathNotFound @3: Void;
  }
}

struct ListAttrRequest {
  arena @0: Text;
  path @1: Text;
}

struct ListAttrResponse {
  attrs @0: List(Text);
}

struct GetAttrRequest {
  attr @0: Text;
  arena @1: Text;
  path @2: Text;
}
struct GetAttrResponse {
  value @0: Text;
}

struct SetAttrRequest {
  attr @0: Text;
  value @1: Text;
  arena @2: Text;
  path @3: Text;
}
struct SetAttrResponse {
}                            