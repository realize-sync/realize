@0x950bed21f9dbff63;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("global::types");

# An entry in the path table.
struct PathTableEntry {
  pathid @0: UInt64;
  mtime @1: Time;
}

# Time as duration since UNIX_EPOCH.
struct Time {
  secs @0: UInt64;
  nsecs @1: UInt32;
}

