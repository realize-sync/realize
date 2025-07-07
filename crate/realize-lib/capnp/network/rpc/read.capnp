@0xe4632a6ed7c19e67;

using Rust = import "/capnpc/rust.capnp";
$Rust.parentModule("network::rpc");

using import "result.capnp".Result;

interface Reader {
  read @0 (req: ReadRequest) -> (result: Result(ReadResponse, ReadError));
  seek @1 (req: SeekResponse) -> (result: Result(SeekResponse, ReadError));
  close @2 () -> ();
}

struct ReadRequest {
  len @0: UInt32;
}

struct ReadResponse {
  data @0: Data;
  atEnd @1: Bool;
}

struct SeekRequest {
  offset @0: UInt64;
}

struct SeekResponse {
  
}

# Read errors, loosely based on I/O errors for ease of conversion.
struct ReadError {
  errno @0: Errno;

  enum Errno {
    other @0;
    genericIo @1;
    unavailable @2;
    permissionDenied @3;
    notADirectory @4;
    isADirectory @5;
    invalidInput @6;
  }
}

