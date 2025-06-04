# Rust Package API Documentation

This rule ensures proper documentation lookup for API of dependencies.

When editing rust code, to find out the right API call to make in a
rust package, look up the package documentation in
file://<PROJECT_WORKSPACE>/target/doc/

To look up the documentation of package PACKAGENAME, go to 
file://<PROJECT_WORKSPACE>/target/doc/PACKAGENAME/index.html

If the files are missing, run "cargo doc" to create it.

