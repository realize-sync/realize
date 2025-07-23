@0xca95085c49f2ff10;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("arena");

# Store retry information about a job.
struct FailedJobTableEntry {
  # Don't retry until that time.
  #
  # Time is seconds since the beginning of the Unix epoch.
  backoffUntilSecs @0: UInt64;

  # Number of times the job failed.
  failureCount @1: UInt32;
}
