@0x8b094bf663d40cea;

using import "realstore.capnp".RealStore;

interface Peer {
  store @0 () -> (store: RealStore);

  # Add history publish/subscribe.
}
