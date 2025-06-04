# Rust Testing Best Practices

## Checks

1. **Test Organization**
   - Place unit tests in the same file as the code being tested
   - Make sure unit tests cover all cases, avoid relying only on integration tests
   - Use integration tests for testing the command line behavior and output
   - Follow the AAA (Arrange-Act-Assert) pattern

2. **Test Coverage**
   - Test both success and error cases
   - Include edge cases and boundary conditions
   - Use property-based testing where appropriate

3. **Test Isolation**
   - Use test-specific types and mocks
   - Avoid shared mutable state between tests
   - Clean up resources after tests


## Examples

### Good

Example module `process.rs`:

```rust

fn process(input: Input) {
  ...
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_success_case() {
        let input = "test";
        
        let result = process(input);
        
        assert_eq!(result, expected);
    }

}
```

### Bad

```rust
#[test]
fn test_with_shared_state() {
    // Bad: Using shared mutable state
    static mut COUNTER: i32 = 0;
    unsafe { COUNTER += 1; }
}

#[test]
fn test_without_cleanup() {
    // Bad: Not cleaning up resources
    let file = File::create("test.txt").unwrap();
    // No cleanup after test
}
```

In file `process_test.rs`:
```
// Bad: this should be in the file process.rs

#[cfg(test)]
mod tests {

    #[test]
    fn test_success_case() {
       ...
    }

}
```

## Rationale

Proper testing practices ensure:
- Code reliability and correctness
- Easy maintenance and refactoring
- Clear documentation through examples
- Confidence in code changes

## References

- [Rust Book - Testing](mdc:https:/doc.rust-lang.org/book/ch11-00-testing.html)
- [Rust Testing Guide](mdc:https:/rust-lang.github.io/book/ch11-00-testing.html)
