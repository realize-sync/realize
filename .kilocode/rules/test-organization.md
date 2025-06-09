# Unit Test Organization

All new code must be thoroughly tested. Prefer unit tests. Add
integration test when necessary.

Write unit tests in the same .rs file as the code, inside an inner
tests module.

Write integration tests in
`crate/<cratename>/tests/<component>_integration_tests.rs`

* Test functions are declared with `#[test]` or `#[tokio::test]`

* Test functions must always return the type `anyhow::Result<()>` and end with `Ok(())`.
* Test functions must not use `unwrap()`. Instead, use the `?` operator for error propagation.

* Test functions describe the component that is tested and the
  situation the component is tested in using name such as
  `<component>_with_<situation>`

* Test functions don't start with `test_`

* Add fixtures in the `tests` module before the tests

* Add helper functions in the `tests` module after the tests

Examples:

```rust

fn do_something(str: String) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn do_something_with_some_test_string() -> anyhow::Result<()> {
       assert_eq!(..., do_something("some_test_string"));

       Ok(())
    }
    ...
}
```


Example, an async function

```rust

async fn do_something_async(str: String) {}

#[cfg(test)]
mod tests {
    ...
    #[tokio::test]
    async fn do_something_async -> anyhow::Result<()> {
       ...

       Ok(())
    }
    ...
}
```

Examples, with a fixture:

```rust

fn do_something_else(str: String) {}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
      ...
    }
    impl Fixture {
      fn setup() -> Self {
        Self { ... }
      }

      fn do_something_useful(&self) -> anyhow::Result<()> {
        ...
      }
    }

    #[test]
    fn do_something_else() -> anyhow::Result<()> {
       let fixture = Fixture::setup();
       fixture.do_something_useful()?;
       ...

       Ok(())
    }

    ...
}
```
