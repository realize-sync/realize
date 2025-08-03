@0xca95085c49f2ff10;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("arena::types");

# Store retry information about a job.
struct FailedJobTableEntry {
  # Number of times the job failed.
  failureCount @1: UInt32;

  retry: union {
    # Don't retry until that time.
    #
    # Time is seconds since the beginning of the Unix epoch.
    after @0: UInt64;

    whenPeerConnects @2: Void;
  }
}
