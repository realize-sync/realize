# Dummy interface for testing connections.

@0xbddeb873b5bc84e8;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("network::testing");

interface Hello {
  hello @0 (name: Text) -> (result: Text);
}

