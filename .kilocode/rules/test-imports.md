# Test Imports Organization

Test-only dependencies and imports must be placed inside the test
module to maintain clear separation between production and test code.

This does not mean that any dependency used in test must be placed
inside of the test module. This only applies to the dependencies that
are *only* used in tests. If a dependency is needed to implement the
feature of the module, it must not only be used in tests.

## Checks

1. **Test Dependencies**
   - Test-only dependencies must be imported inside `mod tests`
   - Test-only imports must not appear in the main module scope
   - Each test module should have its own imports section

2. **Import Organization**
   - Place test imports immediately after `use super::*;`
   - Keep imports specific to test code within test modules

## Examples

### Good
```rust
use std::fs;
use std::path::Path;

pub struct MyStruct {
    // ... production code ...
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;  // test-specific std import
    use tempfile::tempdir;  // test-only dependency
    use assert_unordered::assert_eq_unordered;  // test-only dependency

    #[test]
    fn test_something() {
        // ... test code ...
    }
}
```

### Bad
```rust
use std::fs;
use std::path::Path;
use tempfile::tempdir;  // BAD: test dependency in main scope
use assert_unordered::assert_eq_unordered;  // BAD: test dependency in main scope

pub struct MyStruct {
    // ... production code ...
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_something() {
        // ... test code ...
    }
}
```

## Rationale

Proper test import organization ensures:
- Clear separation between production and test code
- Better code organization and maintainability
- Reduced confusion about which dependencies are used where
- Proper dependency tree for production builds

## References

- [Rust Book - Test Organization](mdc:https:/doc.rust-lang.org/book/ch11-03-test-organization.html)
- [Rust API Guidelines](mdc:https:/rust-lang.github.io/api-guidelines)
