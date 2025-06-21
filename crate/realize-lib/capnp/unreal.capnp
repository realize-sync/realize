@0x950bed21f9dbff63;

# An entry in the directory table.
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
  parent @1: UInt64;
  content @2: ContentRef;
}

struct ContentRef {
  arena @0: Text;
  path @1: Text;
}

struct FileMetadata {
  size @0: UInt64;
  mtime @1: Time;
}

# Time as duration since UNIX_EPOCH.
struct Time {
  secs @0: UInt64;
  nsecs @1: UInt32;
}