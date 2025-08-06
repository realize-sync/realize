# An entry in the directory table.

@0xe1b5a79fba1782e8;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("arena::types");

# An entry in the blob table.
struct BlobTableEntry {
  writtenAreas @0: ByteRanges;

  # Hash of the content, may be missing or inconsistent
  # with the corresponding file entry.
  contentHash @1: Data;
}

# A sequence of byte ranges.
struct ByteRanges {
  ranges @0: List(ByteRange);
}

# A single byte range.
struct ByteRange {
  start @0: UInt64;
  end @1: UInt64;
}

# LRU Queue ID enum
enum LruQueueId {
  workingArea @0;
  protectedArea @1;
  pendingRemoval @2;
}

# An entry in the queue table.
struct QueueTableEntry {
  # First node in the queue (BlobId)
  firstNode @0: UInt64;

  # Last node in the queue (BlobId)
  lastNode @1: UInt64;

  # Total disk usage in bytes
  diskUsage @2: UInt64;
}
