# Rust-Only Code Generation

Only generate code in Rust for this project and store that code in
files with the extension .rs.

## Examples

Bad: Python code:

```python
      def hello_world():
          print("Hello, World!")
```

Good: Rust code

```rust
      fn hello_world() {
          println!("Hello, World!");
      }
```
