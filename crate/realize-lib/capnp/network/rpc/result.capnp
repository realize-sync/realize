@0xcc824aef448f2848;

struct Result(ValueType,ErrorType) {
  union {
    ok @0: ValueType;
    err @1: ErrorType;
  }
}

