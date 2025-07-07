@0xcc824aef448f2848;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("rpc");

struct Result(ValueType,ErrorType) {
  union {
    ok @0: ValueType;
    err @1: ErrorType;
  }
}

