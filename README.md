# Realize

## Overview

Realize is a decentralized file-syncing solution, with support for
partial syncing. Available files are presented as a unified filesystem
that combines remote files, with local data and modifications.

When remote files change, updates are tracked locally and later
synchronized with trusted peers. Peers may choose which files they
keep locally and share with others and which files are only available
through the cache.

This project is written in Rust and supports Linux and MacOS (with
certain limitations).

## **Work In Progress**

> [!IMPORTANT] The Realize project is currently under development and
> is not yet usable. It is in the early stages of design and
> implementation. As such, **it should not be used by anyone**. There
> are no guarantees of backward compatibility. Expect things to break.

## Project Stages

Realize will advance through several key stages:

1. Implement basic file transfer between peers via a command line
   tool. See [movedirs](./movedirs.md) for details. *available*

2. Expose remote files, partially cached locally, as a filesystem. See
   [The Unreal](./unreal.md). *available*

3. Provide a customizable, high-performance merged view of local and
   remote files on Linux, and a read-only view on MacOS. See [The
   Real](./real.md). *in progress*

4. Add a user interface for MacOS and Linux. *planned*

5. Offer installable packages for MacOS and Linux. *planned*

6. Develop a customizable merged view for MacOS. *planned*

7. Include support for Windows. *planned*

For a detailed description of the project architecture and features,
refer to the [design document](./design.md).

## Development

### Requirements

- Linux for the complete system (as it requires
  [inotify](https://man7.org/linux/man-pages/man7/inotify.7.html)); a
  subset of functionality is also available on MacOS.

- Rust for development.

### Getting Started

1. Clone the repository:

   ```bash
   git clone https://github.com/realize-rs/realize.git
   ```

2. Navigate to the project directory:

   ```bash
   cd realize
   ```

3. Build and test the project using Cargo:

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
thread](https://github.com/realize-rs/realize/discussions) or [create
an issue](https://github.com/realize-rs/realize/issues) in the
repository.

## Get Involved

Stay updated as we continue developing the Realize project. Follow us
on GitHub for the latest releases and discussions!
