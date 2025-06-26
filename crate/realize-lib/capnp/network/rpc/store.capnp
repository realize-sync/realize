@0xee767b7022b5de29;

using import "result.capnp".Result;

interface Store {
  # Set of Arenas kept in the store.
  #
  # Might be empty.
  arenas @0 () -> (arenas: List(Text));
  
  # Read a blob from the store
  read @1 (req: ReadRequest) -> (result: Result(ReadResponse, ReadError));

  # Subscribe to notifications to receive and be kept 
  # up-to-date on the store file list
  subscribe @2 (req: SubscribeRequest) -> (result: Result(SubscribeResponse, SubscribeError));
}

struct ReadRequest {
  arena @0: Text;
  path @1: Text;
  offset @2: UInt64;
  size @3: UInt32;
}
struct ReadResponse {
  data @0: Data;
}
struct ReadError {
 message @0:Text;
}



struct SubscribeRequest {
  subscriber @0: Subscriber;
  arenas @1: List(Text);
}

struct SubscribeResponse {
}

struct SubscribeError {
 message @0:Text;
}

interface Subscriber {
  notify @0 (notifications: List(Notification));
}

struct Notification {
  union {
    link @0: Link;
    unlink @1: Unlink;
    catchingUp @2: CatchingUp;
    catchup @3: Link;
    ready @4: Ready;
  }
}

struct CatchingUp {
  arena @0: Text;
}
struct Ready {
  arena @0: Text;
}
struct Link {
  arena @0: Text;
  path @1: Text;
  size @2: UInt64;
  mtime @3: Time;
}

struct Unlink {
  arena @0: Text;
  path @1: Text;
  size @2: UInt64;
  mtime @3: Time;
}

struct Time {
  secs @0: UInt64;
  nsecs @1: UInt32;
}