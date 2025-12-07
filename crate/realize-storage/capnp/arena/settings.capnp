@0xbbbc215dccec9d63;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("arena::types");

struct SettingsTableEntry {
  uuidHigh @0: UInt64;
  uuidLow @1: UInt64;
  diskUsage @2: DiskUsageConfig;
}

struct DiskUsageConfig {
  max @0: BytesOrPercent;
  leave @1: BytesOrPercent;
  trashExpiration @2: Float64;
}

struct BytesOrPercent {
  union {
    bytes @0: UInt64;
    percent @1: UInt32;
  }
}
