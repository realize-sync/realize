# Future Changes to the Spec

This file lists changes that are planned but that haven't been
integrated into the spec yet.

## Create Rust workspace {#workspace}

Create a Rust workspace for the realize project that:
 - uses Rust edition 2024
 - contains a library crate, to include all reusable code
 - contains a command crate for the daemon (that uses the library)
 - contains a command crate for the realize command-line tool (that uses the library)

See the sections:
 - "Components of the system" in the file spec/design.md
 - "Code Organization" in the file spec/design.md

Task list:

1. Create the workspace and the crates (library, daemon and cli).
2. Make the two command-line tools depend on the library crate
3. run ant build to build anything. It should not fail.

## Create initial service {#servicedef}

Define a service for use with tarpc 0.36 as described in
spec/design.md, in the section "Service Definition".

From that section, the service RealizeService should include the
following methods:

- List
- Send
- Finish

It should *not* include any other method.

To test it, write a dummy implementation of that service and write a
test that uses that implementation through an in-process channel
tarpc::transport::channel.

Task list:

1. Put service definition in the library the service module, in the
   library crate in src/model/service.rs (As described in "Code
   Organization" in the file spec/design.md)

2. Put a skeleton implementation in the library in the impl module, in
   src/server.rs (As described in "Code Organization" in the file
   spec/design.md)

3. Use "cargo check" to make sure the code compile. Fix any issues.

4. Add a test inside src/server.rs that create a server and client for
   that service and have them communicate with an in-process channel

5. Use "cargo test" to make sure the test passes. Fix any issues.

## Implement the initial service {#serviceimpl}

Implement the methods List, Send and Finish of RealizeService.

Task list:

1. Implement the skeleton in the lib crate in src/server.rs. See the
   description in the "Service Definition" section of spec/design.md,
   or more generally in the "Overview" section.

2. Use "cargo check" to make sure the code compile. Fix any issues.

2. Extend the existing tests and add new ones. Use assert_fs for
   creating directories and checking files.

5. Use "cargo test" to make sure the test passes. Fix any issues.

## Implement the algorithm {#algoimpl}

Implement the algorithm described in the section "Sync Algorithm: Move
files from A to B" in spec/design.md

The algorithm should take two instances of the RealizeService, defined
in src/model/service.rs, one for A (source) one for B (destination).

The implementation should leave out step 4, which is an optimization,
and treat files that are present in A and partially in B as in step 3,
that is, overwrite the file that's partially in B.

The tests should call the implementation of the service defined in
src/server.rs, using an in-process channel.

1. Read spec/design.md and keep it in mind
2. Create the new module in src/algo/move.rs
3. Add a new function that takes two RealizeService instances to work on
4. Implement that function as described
5. Run "cargo check" to make sure everything compiles, fix any issues
6. Add a test for that function
7. Run "cargo test" to make sure the tests pass, fix any issues

