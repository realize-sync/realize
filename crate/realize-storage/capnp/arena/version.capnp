@0xf2e680ec83fed9db;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("arena::types");

struct Version {
  union {
    modified @0 :ModifiedVersion;
    indexed @1 :Data;
  }
}

struct ModifiedVersion {
  hash @0: Data;
}


