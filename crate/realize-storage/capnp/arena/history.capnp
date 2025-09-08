# Types stored in the index databases

@0xa865be6edb44e079;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("arena::types");

# An entry in the history table
struct HistoryTableEntry {
  union {
    add @0: Add;
    replace @1: Replace;
    remove @2: Remove;
    drop @3: Drop;
    branch @4: Branch;
    rename @5: Rename;
  }

  struct Add {
    path @0: Text;
  }

  struct Replace {
    path @0: Text;
    oldHash @1: Data;
  }

  struct Remove {
    path @0: Text;
    oldHash @1: Data;
  }

  struct Drop {
    path @0: Text;
    oldHash @1: Data;
  }

  struct Branch {
    path @0: Text;
    destPath @1: Text;
    hash @2: Data;
    oldHash @3: Data;
  }
  
  struct Rename {
    path @0: Text;
    destPath @1: Text;
    hash @2: Data;
    oldHash @3: Data;
  }
}

# Time as duration since UNIX_EPOCH.
struct Time {
  secs @0: UInt64;
  nsecs @1: UInt32;
}