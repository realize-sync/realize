---
description: Where to put use statements.
globs: *.rs
alwaysApply: false
---
## Where to put use statements

Add imports needed in Unit test code, to the test module, not to the
containing (non-test) module. Imports needed both in the test module and
its containing module can be added to the containing (non-test) module.

## Name
rust-import

## Description

This rule enforces the correct usage and placement of dependencies in
Rust files containing unit tests.

## Rule

- **Where to add a `use` statement**

  - If the use statement is added for code inside of the test module,
    containing unit tests, add the use statement to that module

  - If the use statement is added for code outside of the test module,
    add the use statement at the top of the file

  - Avoid putting use statements inside of functions

- **How to interpret import errors**

If you see such an error:

```
error[E0432]: unresolved import `assert_fs`
  --> src/algo.rs:10:5
   |
10 | use assert_fs::TempDir;
   |     ^^^^^^^^^ use of undeclared crate or module `assert_fs`
```

but you see that `assert_fs` is listed in `Cargo.toml` in the
`dev-dependencies` section, then check where the use statement is,
following the rule above.

## Rationale

Dev dependencies are only available:

 - from sections #[cfg(test)], #[cfg(bench)], #[cfg(example)]

 - in files tests/**.rs

They are *NOT* available in production code.

Avoid compilation errors by following this rule.

## Examples

✅ **Correct:**

`src/mypackage.rs`:
```rust
use serde::Serialize; // OK: production dependency

// Some code that uses serde::Serialize

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq; // OK: dev dependency imported inside test module

    #[test]
    fn test_add() {
        assert_eq!(2 + 2, 4);
    }
}
```

❌ **Incorrect:**

`src/mypackage.rs`:
```rust
use pretty_assertions::assert_eq; // ERROR: test dependency imported at file header

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        assert_eq!(2 + 2, 4);
    }
}
```

## References

- [Rust By Example: Development dependencies](https://doc.rust-lang.org/rust-by-example/testing/dev_dependencies.html)
- [Cargo Book: Specifying dependencies](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html)