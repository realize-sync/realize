# Types stored in the engine database

@0xed5ff5c960536585;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("arena::types");

# An entry in the mark table.
struct MarkTableEntry {
  mark @0: Mark = watch;
}

# Mark types for file operations.
enum Mark {
  watch @0;
  keep @1;
  own @2;
} 