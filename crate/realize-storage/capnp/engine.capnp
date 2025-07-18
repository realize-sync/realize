# Types stored in the engine database

@0xed5ff5c960536585;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("engine");

# An entry in the decision table.
struct DecisionTableEntry {
  decision @0: Decision;
}

# Decision types for file operations.
enum Decision {
  # Move from cache to arena root (as a regular file)
  realize @0;
  
  # Move from arena root to cache
  unrealize @1;
  
  # Update the file from another peer and store it in the cache
  updateCache @2;
} 