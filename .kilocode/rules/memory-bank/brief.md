Realize is a non-centralized file synchronization utility written in
rust. The overall goal is to sync files partially between peers. Some
peers may store some files, other hosts other files.

Each peer has access to both local and remote files through a custom
filesystem. Local modifications are shared with remote peers.

See [@/spec/index.md](../../spec/index.md) for the design the
different subsystems. See [@/spec/future.md](../../spec/future.md) for
planned code and spec changes.
