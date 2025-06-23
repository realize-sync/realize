@0x8b094bf663d40cea;

using import "store.capnp".Store;

interface ConnectedPeer {
  # Return a handle on the peer store.
  store @0 () -> (store: Store);

  # Make store available to the peer.
  #
  # This in effect reverses the client/server relationship, in
  # case peer which cannot be connected to want to share their
  # store content.
  register @1 (store:Store);
}
