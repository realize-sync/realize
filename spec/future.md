# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## upgrade to redb 3 {#redb3}

Upgrade redb to version 3.

## configure "auth" logs {#logauth}

setup configuration so that connection attempts and auth error/accept
can easily be singled out.

Currently connection attempts appear as, for example:

```
...DEBUG realize_network::network] 199.45.155.104:49506: connection rejected: received corrupt message of type InvalidContentType
```

## Re-design churten {#nochurten}

With the latest changes, churten doesn't make much sense anymore; it's
just download. Also, important information is missing such as:

- initial hashing, which could take a while
- download/verify/evict
- realize/unrealize
- connect/disconnect
- local changes caused by remote notifications (out-of-date, deletion)

There might be a need for an "audit" concept to log some of these.

Possibly use or integrate with tracing.

## Trim history {#trimhistory}

Decide on rules for trimming history.

## Support accessing large files without storing them {#largedl}

Currently, when accessing remote files, the data is always downloaded.
If the file is kept as long as it's open, even if it's too large to
fit in the working area. That's wrong.

## Switch from file to dir and back {#filetodir}

Test and document what happens if the same name is a directory on some
peers and a file in others.

## Recover inode range in Arena cache {#inoderange}

When allocating a inode range in
[cache.rs](../crate/realize-storage/src/unreal/cache.rs), there are
two databases working together, the database of the global cache and
the database of the arena cache. Since 2-phase commit is not supported
between redb database, we can end up in a situation where the inode
range allocated for the arena by do_alloc_inode_range is forgotten by the
arena cache, as the transaction of the arena cache is aborted after
the transaction of the global cache has been submitted.

One solution to avoid that is to update the logic of
do_alloc_inode_cache so that if it's called by an arena cache that's
forgotten about an previously-allocated range, that
previously-allocated range is returned instead. It is possible, since
range only increase.

Task list:

- Have the arena cache pass the end of the exhausted range (or 0 the
  first time) eventually to do_alloc_inode_range

- before allocating a new range do_alloc_inode_range does a range
  search with the exhausted range `exhausted_range_end+1..` and if
  there is one range returned that's allocated to the current arena,
  return that.

This also allows reusing ranges if the entire arena database is deleted.

Task list:

 1. pass the end of the exhausted range from the arena cache to
    do_alloc_inode_range. Make sure the range compile.

 2. update do_alloc_inode_range to do a range check. Add a unit test.
    Make sure the test passes.

 3. add a test where all caches instances are dropped and re-created
    after deleting one of the arena cache, make sure the inode range
    of the deleted arena cache correspond to the previous range.


## IPV6 + IPV4 {#ipv64}

Localhost is currently resolved to ipv6 address, which isn't what's
expected in the tests, so all tests use 127.0.0.1.

This isn't right; it should be possible to specify localhost (or any
address that resolves to both an ipv6 address and an ipv4 address) and
have it work normally (try ipv6, fallback to ipv4).

This is normally automatic, I expect, but the custom transformation to
SocketAddr screws that up.

## Compression {#compress}

Implement compression at the network connection level.

This won't help much for files that are already compressed (audio or
video).
