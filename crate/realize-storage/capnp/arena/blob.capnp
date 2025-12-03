@0xe1b5a79fba1782e8;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("arena::types");

using import "version.capnp".Version;
using import "time.capnp".Time;

# An entry in the blob table.
struct BlobTableEntry {
  writtenAreas @0: ByteRanges;

  # Version of the content
  version @1: Version;

  # Size of the content
  contentSize @6: UInt64;

  # If true, content was verified against hash.
  verified @7: Bool;

  # Queue ID enum
  queue @2: LruQueueId;

  # Next blob in the queue (PathId)
  next @3: UInt64;

  # Previous blob in the queue (PathId)
  prev @4: UInt64;

  # Disk usage in bytes
  diskUsage @5: UInt64;

  # Time at which the blob was created; especially
  # useful for archives
  timestamp @8: Time;
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
  cached @0;
  protected @1;
  archived @2;
}

# An entry in the queue table.
struct QueueTableEntry {
  # First node in the queue (PathId)
  head @0: UInt64;

  # Last node in the queue (PathId)
  tail @1: UInt64;

  # Total disk usage in bytes
  diskUsage @2: UInt64;
}
