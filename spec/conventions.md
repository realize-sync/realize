# Project Rules and Conventions

- Only generate Rust code, capnp types, toml configuration files. No other language is acceptable.

- All FS and I/O operations must be async outside of test code or classes whose name ends with Blocking. Use tokio::fs instead of std::fs. Run unavoidable blocking code instead of tokio::task::spawn_blocking.

- When moving code, move the comments that go with it as well.


- To add a new dependency, use the command "cargo add -p <crate> <dependency> --features ''" Let cargo-add choose the version.

- To test the code, prefer unit tests, defined in the same .rs file inside of a "tests" module.

- When adding or changing code, always add or update the test code in the same .rs file.

- Keep test name short and descriptive of the action that is being tests. Example: "catchup_detects_new_files". Do not prefix tests names with test_

- All tests must return anyhow::Error<()>. Example:
```
[#test]
fn foo_does_bar() -> anyhow::Result<()> {
  assert!(is_bar(foo()?));

  Ok(())
}
```

- Tests often need to be async. Example of an async test:
```
[#tokio::test]
async fn foo_does_bar() -> anyhow::Result<()> {
  assert!(is_bar(foo().await?));

  Ok(())
}
```

- Use integration tests to test changes to command-line tools.

- Don't use .unwrap() or .expect(), not even in test code, not without explicit confirmation and a comment explaining why.

- Don't create `mod.rs` file. The definition for the module "foo::bar" goes into `foo/bar.rs` (NOT `foo/bar/mod.rs`) and the definition for the module "foo" goes into `foo.rs` (NOT `foo/mod.rs`)

- Fix *all* warnings reported by the compiler, even those that look innocuous. Exception: dead code warning when you know you're going to use the code in the next step.

- Use log::debug!() for logging. Don't add log::warn!() or log::error!() unless it's part of the task.

- Debugging tip: To debug a failing test, run just that test with RUST_LOG=debug and --nocapture. For example, to debug the test foo_does_bar defined in foo.rs, run "RUST_LOG=debug cargo test --lib foo_does_bar -- --nocapture"

- Import organization: put imports (use statements) in alphabetical order, don't separate groups by newlines.

- Import modules and classes within modules, each module in its own statement. DO:
```
use std::path::{Path, PathBuf}; // OK
use std::mem; // OK
```
DON'T:
```
use std::{mem, path::{Path, PathBuf}}; // NOT OK
use std::path::Path;
use std::path::PathBuf; // NOT OK
```

- To return errors, return anyhow::Result<> (an alias for Result<, anyhow::Error>) unless there is a reason to return another type such as classes or functions that are called within an environment that require a specific error type. For example, code that will run inside of a filesystem must eventually transform all errors to io::Error, so they return custom error types crated with thiserror.

- Document *all* public struct, fields, enums and functions. Consider documenting private ones as well unless they're trivial to understand.

- Avoid using `.clone()` when ownership can be transferred

- Prefer references when possible

- Use `&str` instead of String for string literals when possible

- Ensure only one mutable reference exists at a time

- Avoid unnecessary mutabilitiy

- Use &mut only when data needs to e modified

- Add explicit lifetime annotations when compiler cannot infer them

- Use descriptive lifetime names (e.g., `'a`, `'static`)

- Ensure lifetime parameters are properly constrained

- Use `Arc` for shared ownership across threads

- Use `Rc` for shared ownership within a single thread

- Consider using `Weak` references to break reference cycles

- *never* use unsafe blocks

- Test everything. Prefer unit tests, placed in the same file as the code being tests, but add integrations tests as needed.

- Make sure unit tests cover all cases, avoid relying only no integration tests

- When writing tests, follow the AAA (Arrange-Act-Assert) pattern

- In tests, include both success and error cases, as well as edge cases and boundary conditions

- Integration tests go into crates/<crate-name>/tests/<name>_integration.rs

- When common code is needed between tests, create a fixture. For example:
```
    struct Fixture {
        mytype: MyType<
    }
    impl Fixture {
        async fn setup() -> anyhow::Result<Fixture> {
            let _ = env_logger::try_init();
            let mytype = MyType::new();
            Ok(Self {mytype})
        }
    }

    #[tokio::test]
    fn mytype_dose_something() -> anyhow::Result<()> {
        let fixture = Fixture::setup().await?;
        assert!(fixture.mytype.do_something()?);

        Ok(())
    }

```

- When adding an import only needed in test code, remember to add the use statement inside of the `tests` module, not the enclosing module. If you don't follow that rule, you'll get warnings from the compiler about unnecessary imports.

- Enable logger in fixtures and any tests that need it with `let _ = env_logger::try_init();`

- Use appropriate trait bounds

- Prefer `where` clauses for complex bounds

- Use `+` for multiple trait bounds

- Consider using `trait_alias` for common bound combinations

- Use associated types for type relationships

- Implement `Default` for associated types

- Document associated type constraints

- Use `type` aliases for complex associated types

- Add [#derive(...)] to structs as appropriate. Often add Debug, Clone, PartialEq and Eq.

- Use meaningful type parameter names

- Document type parameter constraints

- Consider using `const` generics where appropriate

- Use `PhantomData` for type-level programming

- In tests, Use `assert_fs::TempDir` to create temporary directories and files, NOT `assert_fs::TempDir`
