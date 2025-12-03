@0xb325ed7db169917a;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("arena::types");

# Time as duration since UNIX_EPOCH.
struct Time {
  secs @0: UInt64;
  nsecs @1: UInt32;
}


