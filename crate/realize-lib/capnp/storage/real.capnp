@0x950bed21f9dbff63;
# Types stored in the index databases

# An entry in the file table.
struct FileTableEntry {
  hash @0: Data;
  mtime @1: Time;
  size @2: UInt64;
}

# An entry in the history table
struct HistoryTableEntry {
  kind @0: Kind;
  enum Kind {

    # File content has been set.
    #
    # Check the file table for the content and
    # metadata.
    add @0;

    # File content has been modified.
    #
    # Check the file table for the content and
    # metadata.
    replace @1;

    # File has been removed.
    remove @2;
  }

  path @1: Text;

  # Hash of the content that was removed
  # (kind=remove) or replaced (kind=replace).
  oldHash @2: Data;
}

# Time as duration since UNIX_EPOCH.
struct Time {
  secs @0: UInt64;
  nsecs @1: UInt32;
}