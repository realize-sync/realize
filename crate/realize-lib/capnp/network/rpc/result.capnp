@0xcc824aef448f2848;

struct Result(ValueType) {
  union {
    ok @0: ValueType;
    error @1: Error;
  }
}

struct Error {
  union {
    badRequest @0: Text;
    io @1: Text;
    rsync :group {
      operation @2: RsyncOperation;
      message @3: Text;
    }
    other @4: Text;
    hashMismatch @5: Void;
  }
}

enum RsyncOperation {
  diff @0;
  apply @1;
  sign @2;
}