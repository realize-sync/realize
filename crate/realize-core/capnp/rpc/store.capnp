@0xee767b7022b5de29;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("rpc");

using import "result.capnp".Result;

interface Store {
  # Set of Arenas kept in the store.
  #
  # Might be empty.
  arenas @0 () -> (arenas: List(Text));
  
  # Subscribe to notifications to receive and be kept 
  # up-to-date on the store file list
  subscribe @1 (req: SubscribeRequest) -> (result: Result(SubscribeResponse, SubscribeError));

  # Read data from a file
  #
  # In case of errors, ReadCallback.finished()
  # might be called immediately.
  #
  # Currently read is mapped as-is from the filesystem, even
  # if the data changes while it's being read.
  # TODO: add option to check hash and file stability.
  read @2 (req: ReadRequest, cb: ReadCallback) -> ();

  # Send a rsync signature for a range of a file and
  # get back a delta.
  rsync @3 (req: RsyncRequest) -> (res: RsyncResponse);

  # Return a copy of the store that apply the
  # given rate limit to all data that is sent.
  #
  # Note that if the current store already has a rate limit,
  # this overwrites it and the two rate limits are counted
  # separately.
  withRateLimit @4 (rateLimit: Float64) -> (store: Store);
}

struct RsyncRequest {
  arena @0: Text;
  path @1: Text;
  range @2: ByteRange;
  sig @3: Data;
}

struct RsyncResponse {
  delta @0: Data;
}

struct ReadRequest {
  arena @0: Text;
  path @1: Text;

  # Offset from start to start reading from.
  startOffset @2: UInt64 = 0;

  # Read at most that many bytes, less if we reach EOF.
  #
  # 0 means read to EOF.
  limit @3: UInt64 = 0;

}

interface ReadCallback {
  # Send one chunk of data. The server chooses the size of the chunks.
  #
  # Chunks may come out of order. Always check the offset.
  chunk @0 (offset: UInt64, data :Data) -> stream;

  # Report that the stream is finished,.
  #
  # If an error is detected, the stream ends with err set without having
  # read everything.
  finish @1 (result: Result(ReadFinished, IoError)) -> ();
}

struct ReadFinished {}

# Read errors, loosely based on I/O errors for ease of conversion.
struct IoError {
  errno @0: Errno;

  enum Errno {
    other @0;
    genericIo @1;
    unavailable @2;
    permissionDenied @3;
    notADirectory @4;
    isADirectory @5;
    invalidInput @6;
    closed @7;
    aborted @8;
    notFound @9;
    resourceBusy @10;
    invalidPath @11;
  }
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
    drop @7: Drop;
    catchupStart @3: CatchupStart;
    catchup @4: Catchup;
    catchupComplete @5: CatchupComplete;
    connected @6: Connected;
    branch @8: Branch;
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
struct Drop {
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

struct Branch {
  arena @0: Text;
  source @1: Text;
  dest @2: Text;
  hash @3: Data;
  oldHash @4: Data;
}

struct Time {
  secs @0: UInt64;
  nsecs @1: UInt32;
}

struct Uuid {
  lo @0: UInt64;
  hi @1: UInt64;
}

struct ByteRange {
  start @0: UInt64;
  end @1: UInt64;
}