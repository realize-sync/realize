@0xeaa8f5d5c6d6c86c;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("arena::types");

# A new simplified DirTableEntry that only contains mtime
struct DirtableEntry {
  mtime @0: Time;
}

# A union that can be either a FileTableEntry or a DirtableEntry
struct CacheTableEntry {
  union {
    file @0: FileTableEntry;
    dir @1: DirtableEntry;
  }
}

# An entry in the file table.
struct FileTableEntry {
  size @0: UInt64;
  mtime @1: Time;
  path @2: Text;
  hash @3: Data;
  # may be empty
  outdatedBy @4: Data;
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


