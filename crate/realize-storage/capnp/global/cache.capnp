@0x950bed21f9dbff63;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("global::types");

# An entry in the path table.
struct PathTableEntry {
  mtime @0: Time;
  subdirs @1: List(Subdir);
}

struct Subdir {
  name @0: Text;
  inode @1: UInt64;
}

struct ArenaTableEntry {
  prefix @0: UInt8;
  datadir @1: Data;
}

# Time as duration since UNIX_EPOCH.
struct Time {
  secs @0: UInt64;
  nsecs @1: UInt32;
}
