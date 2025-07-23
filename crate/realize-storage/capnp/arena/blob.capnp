# An entry in the directory table.

@0xe1b5a79fba1782e8;


using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("arena::types");

# An entry in the blob table.
struct BlobTableEntry {
  writtenAreas @0: ByteRanges;
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
