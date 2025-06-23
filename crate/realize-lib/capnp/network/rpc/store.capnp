@0xee767b7022b5de29;

using import "result.capnp".Result;

interface Store {
  # Set of Arenas kept in the store.
  #
  # Might be empty.
  arenas @0 () -> (arenas: List(Text));
  
  # Read a blob from the store
  read @1 (req: ReadRequest) -> (result: Result(Data));

  # Subscribe to notifications to receive and be kept 
  # up-to-date on the store file list
  subscribe @2 (req: SubscribeRequest) -> (result: Result(SubscribeResponse));
}

struct ReadRequest {
  arena @0: Text;
  path @1: Text;
  offset @2: UInt64;
  size @3: UInt32;
}

struct SubscribeRequest {
  subscriber @0: Subscriber;
  arena @1: List(Text);
}

struct SubscribeResponse {
}

interface Subscriber {
  notify @0 (notification: List(Notification));
}

struct Notification {
  union {
    link: group {
      arena @0: Text;
      path @1: Text;
      size @2: UInt64;
      mtime @3: Time;
    }
    unlink: group {
      arena @4: Text;
      path @5: Text;
      mtime @6: Time;
    }
    catchingUp: group {
      arena @7: Text;
    }
    catchup: group {
      arena @8: Text;
      path @9: Text;
      size @10: UInt64;
      mtime @11: Time;    
    }
    ready: group {
      arena @12:Text; 
   }
  }
}

struct Time {
  secs @0: UInt64;
  nsecs @1: UInt64;
}