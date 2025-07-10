# An entry in the directory table.

@0x950bed21f9dbff63;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("storage::unreal");

struct DirTableEntry {
  union {
    regular @0: ReadDirEntry;
    dot :group {
      mtime @1: Time;
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
  metadata @0: FileMetadata;
  content @1: FileContent;
  parent @2: UInt64;
}

struct FileContent {
  arena @0: Text;
  path @1: Text;
  hash @2: Data;
}

struct FileMetadata {
  size @0: UInt64;
  mtime @1: Time;
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