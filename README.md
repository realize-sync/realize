[![CI](https://github.com/realize-sync/realize/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/realize-sync/realize/actions/workflows/ci.yml)

# Realize

## Overview

Realize is a decentralized file-syncing solution, with support for
partial syncing. Available files are presented as a unified filesystem
that combines remote files with local data and modifications.

When remote files change, updates are tracked locally and later
synchronized with trusted peers. Peers may choose which files they
keep locally and share with others and which files are only available
through the cache.

This project is written in Rust and supports Linux and MacOS (through
FUSE). At this point, it is only available as a command-line tool.

## Design

For a detailed description of the project architecture and features,
refer to the [design document](spec/design.md).

## Development

### Requirements

- Linux or MacOS with MacFUSE or FUSE-t installed.
- Rust development environment

### Getting Started

1. [Install Cap'n Proto tools](https://capnproto.org/install.html)

2. Clone the repository:

   ```bash
   git clone https://github.com/realize-sync/realize.git
   ```

3. Navigate to the project directory:

   ```bash
   cd realize
   ```

4. Build and test the project using Cargo:

   ```bash
   cargo test
   ```

## License

Realize is distributed under either of the following licenses at your
discretion:

- The MIT License
- The Apache License (Version 2.0)

For more details, see [LICENSE-MIT](LICENSE-MIT) and
[LICENSE-APACHE](LICENSE-APACHE).

## Contact

For questions or support, please [open a discussion
thread](https://github.com/realize-sync/realize/discussions) or [create
an issue](https://github.com/realize-sync/realize/issues) in the
repository.
