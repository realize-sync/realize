@0xeaa8f5d5c6d6c86c;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("arena::types");

struct DirTableEntry {
  union {
    regular @0: ReadDirEntry;
    dot :group {
      mtime @1: Time;
    }
    dotDot :group {
      parent @2: UInt64;
    }
  }
}
struct ReadDirEntry {
  inode @0: UInt64;
  assignment @1: InodeAssignment;
}

enum InodeAssignment {
  file @0;
  directory @1;
}

# An entry in the file table.
struct FileTableEntry {
  size @0: UInt64;
  mtime @1: Time;
  path @2: Text;
  hash @3: Data;
  blob @4: UInt64;
  # may be empty
  outdatedBy @5: Data;
}

struct PeerTableEntry {
  uuidHi @0: UInt64;
  uuidLo @1: UInt64;
}

# Time as duration since UNIX_EPOCH.
struct Time {
  secs @0: UInt64;
  nsecs @1: UInt32;
}


