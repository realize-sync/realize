@0xee767b7022b5de29;

using import "result.capnp".Result;

interface RealStore {
  list @0 (arena: Text, options: Options) ->
      (result: Result(List(RealFile)));
}

struct RealFile {
  path @0: Text;
  size @1: UInt64;
  mtime @2: Time;
}

struct Time {
  secs @0: UInt64;
  nsecs @1: UInt32;
}

struct Options {
  ignorePartial @0: Bool;
}