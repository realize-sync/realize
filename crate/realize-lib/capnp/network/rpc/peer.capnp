@0x8b094bf663d40cea;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("network::rpc");

using import "store.capnp".Store;

interface ConnectedPeer {
  # Return a handle on the peer store.
  store @0 () -> (store: Store);
}
