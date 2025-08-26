@0x8b094bf663d40cea;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("rpc");

using import "store.capnp".Store;

interface ConnectedPeer {
  store @0 () -> (store: Store); # Return a handle on the store.

  register @1 (store: Store) -> (); # Register another peer's store
}
