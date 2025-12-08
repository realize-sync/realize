# Future work

Each section describes a planned change. Sections should be tagged,
for easy reference, and end with a detailled and numbered task list.

## upgrade to redb 3 {#redb3}

Upgrade redb to version 3.

## ignore mark replaces exclude {#ignore}

Introduce the mark "ignore". Ignored files:
 - won't be returned by the filesystem, neither as local nor as remote
 - won't be preindexed or indexed
 - won't be downloaded, realized or unrealized

They'll still be stored as peer files.

Changing this mark to something else than ignore will mean choosing a
default remote version if there are more than one.

Use this mark to replace exclude.

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
