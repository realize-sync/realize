@0xee767b7022b5de29;

using import "result.capnp".Result;

interface Store {
  # Set of Arenas kept in the store.
  #
  # Might be empty.
  arenas @0 () -> (arenas: List(Text));
  
  # Subscribe to notifications to receive and be kept 
  # up-to-date on the store file list
  subscribe @1 (req: SubscribeRequest) -> (result: Result(SubscribeResponse, SubscribeError));
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
  arena @1: Text;
  progress @2: SubscriberProgress;
}

struct SubscribeResponse {
}

struct SubscriberProgress {
  uuid @0: Uuid;
  lastSeen @1: UInt64;
}

struct SubscribeError {
 message @0:Text;
}

interface Subscriber {
  notify @0 (notifications: List(Notification));
}

struct Notification {
  union {
    add @0: Add;
    replace @1: Replace;
    remove @2: Remove;
    catchupStart @3: CatchupStart;
    catchup @4: Catchup;
    catchupComplete @5: CatchupComplete;
    connected @6: Connected;
  }
}

struct Add {
  index @0: UInt64;
  arena @1: Text;
  path @2: Text;
  size @3: UInt64;
  mtime @4: Time;
  hash @5: Data;
}
struct Replace {
  index @0: UInt64;
  arena @1: Text;
  path @2: Text;
  size @3: UInt64;
  mtime @4: Time;
  hash @5: Data;
  oldHash @6: Data;
}
struct Remove {
  index @0: UInt64;
  arena @1: Text;
  path @2: Text;
  oldHash @3: Data;
}
struct CatchupStart {
  arena @0: Text;
}
struct Catchup {
  arena @0: Text;
  path @1: Text;
  size @2: UInt64;
  mtime @3: Time;
  hash @4: Data;
}
struct CatchupComplete {
  arena @0: Text;
  index @1: UInt64;
}
struct Connected {
  arena @0: Text;
  uuid @1: Uuid;
}

struct Time {
  secs @0: UInt64;
  nsecs @1: UInt32;
}

struct Uuid {
  lo @0: UInt64;
  hi @1: UInt64;
}