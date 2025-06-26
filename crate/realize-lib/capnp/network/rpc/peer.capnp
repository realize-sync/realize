@0x8b094bf663d40cea;

using import "store.capnp".Store;

interface ConnectedPeer {
  # Return a handle on the peer store.
  store @0 () -> (store: Store);
}
