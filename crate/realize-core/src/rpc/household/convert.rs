use crate::rpc::store_capnp;
use crate::rpc::store_capnp::notification;
use realize_storage::{Notification, StorageError};
use realize_types::{self, Arena, ByteRange, Hash, Path, UnixTime};
use std::collections::HashSet;
use uuid::Uuid;

pub(crate) fn storage_to_capnp_err(err: StorageError) -> capnp::Error {
    capnp::Error::failed(err.to_string())
}

pub(crate) fn parse_arena(reader: capnp::text::Reader<'_>) -> Result<Arena, capnp::Error> {
    Ok(Arena::from(reader.to_str()?))
}

pub(crate) fn parse_arena_set(
    arenas: capnp::text_list::Reader<'_>,
) -> Result<HashSet<Arena>, capnp::Error> {
    let mut set = HashSet::new();
    for arena in arenas.iter() {
        set.insert(parse_arena(arena?)?);
    }
    Ok(set)
}

pub(crate) fn parse_uuid(reader: store_capnp::uuid::Reader<'_>) -> Uuid {
    Uuid::from_u64_pair(reader.get_hi(), reader.get_lo())
}

pub(crate) fn parse_range(reader: store_capnp::byte_range::Reader<'_>) -> ByteRange {
    ByteRange::new(reader.get_start(), reader.get_end())
}

pub(crate) fn parse_mtime(reader: store_capnp::time::Reader<'_>) -> UnixTime {
    UnixTime::new(reader.get_secs(), reader.get_nsecs())
}

pub(crate) fn parse_path(reader: capnp::text::Reader<'_>) -> Result<Path, capnp::Error> {
    Path::parse(reader.to_str()?).map_err(|e| capnp::Error::failed(e.to_string()))
}

pub(crate) fn parse_hash(hash: &[u8]) -> Result<Hash, capnp::Error> {
    let hash: [u8; 32] = hash
        .try_into()
        .map_err(|_| capnp::Error::failed("invalid hash".to_string()))?;

    Ok(Hash(hash))
}

pub(crate) fn fill_uuid(mut builder: store_capnp::uuid::Builder<'_>, uuid: &Uuid) {
    let (hi, lo) = uuid.as_u64_pair();
    builder.set_hi(hi);
    builder.set_lo(lo);
}

pub(crate) fn fill_byterange(mut builder: store_capnp::byte_range::Builder<'_>, range: &ByteRange) {
    builder.set_start(range.start);
    builder.set_end(range.end);
}

pub(crate) fn fill_add(
    mut builder: store_capnp::add::Builder<'_>,
    arena: Arena,
    index: u64,
    path: &realize_types::Path,
    size: u64,
    mtime: &realize_types::UnixTime,
    hash: &realize_types::Hash,
) {
    builder.set_arena(arena.as_str());
    builder.set_index(index);
    builder.set_path(path.as_str());
    builder.set_size(size);
    builder.set_hash(&hash.0);
    fill_time(builder.init_mtime(), mtime);
}

pub(crate) fn fill_replace(
    mut builder: store_capnp::replace::Builder<'_>,
    arena: Arena,
    index: u64,
    path: &realize_types::Path,
    size: u64,
    mtime: &realize_types::UnixTime,
    hash: &realize_types::Hash,
    old_hash: &realize_types::Hash,
) {
    builder.set_arena(arena.as_str());
    builder.set_index(index);
    builder.set_path(path.as_str());
    builder.set_size(size);
    builder.set_hash(&hash.0);
    builder.set_old_hash(&old_hash.0);
    fill_time(builder.init_mtime(), mtime);
}

pub(crate) fn fill_remove(
    mut builder: store_capnp::remove::Builder<'_>,
    arena: Arena,
    index: u64,
    path: &realize_types::Path,
    old_hash: &realize_types::Hash,
) {
    builder.set_arena(arena.as_str());
    builder.set_index(index);
    builder.set_path(path.as_str());
    builder.set_old_hash(&old_hash.0);
}

pub(crate) fn fill_drop(
    mut builder: store_capnp::drop::Builder<'_>,
    arena: Arena,
    index: u64,
    path: &realize_types::Path,
    old_hash: &realize_types::Hash,
) {
    builder.set_arena(arena.as_str());
    builder.set_index(index);
    builder.set_path(path.as_str());
    builder.set_old_hash(&old_hash.0);
}

pub(crate) fn fill_catchup(
    mut builder: store_capnp::catchup::Builder<'_>,
    arena: Arena,
    path: &realize_types::Path,
    size: u64,
    mtime: &realize_types::UnixTime,
    hash: &realize_types::Hash,
) {
    builder.set_arena(arena.as_str());
    builder.set_path(path.as_str());
    builder.set_size(size);
    builder.set_hash(&hash.0);
    fill_time(builder.init_mtime(), mtime);
}

pub(crate) fn fill_branch(
    mut builder: store_capnp::branch::Builder<'_>,
    arena: Arena,
    index: u64,
    source: &realize_types::Path,
    dest: &realize_types::Path,
    hash: &realize_types::Hash,
) {
    builder.set_arena(arena.as_str());
    builder.set_index(index);
    builder.set_source(source.as_str());
    builder.set_dest(dest.as_str());
    builder.set_hash(&hash.0);
}

pub(crate) fn fill_rename(
    mut builder: store_capnp::rename::Builder<'_>,
    arena: Arena,
    index: u64,
    source: &realize_types::Path,
    dest: &realize_types::Path,
    hash: &realize_types::Hash,
) {
    builder.set_arena(arena.as_str());
    builder.set_index(index);
    builder.set_source(source.as_str());
    builder.set_dest(dest.as_str());
    builder.set_hash(&hash.0);
}

pub(crate) fn fill_time(
    mut mtime_builder: store_capnp::time::Builder<'_>,
    mtime: &realize_types::UnixTime,
) {
    mtime_builder.set_secs(mtime.as_secs());
    mtime_builder.set_nsecs(mtime.subsec_nanos());
}

pub(crate) fn fill_notification(notif: &Notification, notif_builder: notification::Builder<'_>) {
    match notif {
        Notification::Add {
            arena,
            index,
            path,
            size,
            mtime,
            hash,
        } => fill_add(
            notif_builder.init_add(),
            *arena,
            *index,
            path,
            *size,
            mtime,
            hash,
        ),

        Notification::Replace {
            arena,
            index,
            path,
            size,
            mtime,
            hash,
            old_hash,
        } => fill_replace(
            notif_builder.init_replace(),
            *arena,
            *index,
            path,
            *size,
            mtime,
            hash,
            old_hash,
        ),

        Notification::Remove {
            arena,
            index,
            path,
            old_hash,
        } => fill_remove(notif_builder.init_remove(), *arena, *index, path, old_hash),

        Notification::Drop {
            arena,
            index,
            path,
            old_hash,
        } => fill_drop(notif_builder.init_drop(), *arena, *index, path, old_hash),

        Notification::Catchup {
            arena,
            path,
            size,
            mtime,
            hash,
        } => fill_catchup(
            notif_builder.init_catchup(),
            *arena,
            path,
            *size,
            mtime,
            hash,
        ),

        Notification::CatchupStart(arena) => {
            notif_builder.init_catchup_start().set_arena(arena.as_str())
        }

        Notification::CatchupComplete { arena, index } => {
            let mut builder = notif_builder.init_catchup_complete();
            builder.set_arena(arena.as_str());
            builder.set_index(*index);
        }

        Notification::Connected { arena, uuid } => {
            let mut builder = notif_builder.init_connected();
            builder.set_arena(arena.as_str());
            fill_uuid(builder.init_uuid(), &uuid);
        }

        Notification::Branch {
            arena,
            source,
            dest,
            hash,
            index,
        } => fill_branch(
            notif_builder.init_branch(),
            *arena,
            *index,
            source,
            dest,
            hash,
        ),
        Notification::Rename {
            arena,
            source,
            dest,
            hash,
            index,
        } => fill_rename(
            notif_builder.init_rename(),
            *arena,
            *index,
            source,
            dest,
            hash,
        ),
    }
}

pub(crate) fn parse_notification(
    n: store_capnp::notification::Reader<'_>,
) -> Result<Notification, capnp::Error> {
    Ok(match n.which()? {
        notification::Which::Add(add) => {
            let add = add?;

            Notification::Add {
                arena: parse_arena(add.get_arena()?)?,
                index: add.get_index(),
                path: parse_path(add.get_path()?)?,
                size: add.get_size(),
                mtime: parse_mtime(add.get_mtime()?),
                hash: parse_hash(add.get_hash()?)?,
            }
        }
        notification::Which::Replace(replace) => {
            let replace = replace?;

            Notification::Replace {
                arena: parse_arena(replace.get_arena()?)?,
                index: replace.get_index(),
                path: parse_path(replace.get_path()?)?,
                mtime: parse_mtime(replace.get_mtime()?),
                size: replace.get_size(),
                hash: parse_hash(replace.get_hash()?)?,
                old_hash: parse_hash(replace.get_old_hash()?)?,
            }
        }
        notification::Which::Remove(remove) => {
            let remove = remove?;

            Notification::Remove {
                arena: parse_arena(remove.get_arena()?)?,
                index: remove.get_index(),
                path: parse_path(remove.get_path()?)?,
                old_hash: parse_hash(remove.get_old_hash()?)?,
            }
        }
        notification::Which::Drop(drop) => {
            let drop = drop?;

            Notification::Drop {
                arena: parse_arena(drop.get_arena()?)?,
                index: drop.get_index(),
                path: parse_path(drop.get_path()?)?,
                old_hash: parse_hash(drop.get_old_hash()?)?,
            }
        }
        notification::Which::CatchupStart(start) => {
            Notification::CatchupStart(parse_arena(start?.get_arena()?)?)
        }
        notification::Which::Catchup(catchup) => {
            let catchup = catchup?;

            Notification::Catchup {
                arena: parse_arena(catchup.get_arena()?)?,
                path: parse_path(catchup.get_path()?)?,
                size: catchup.get_size(),
                mtime: parse_mtime(catchup.get_mtime()?),
                hash: parse_hash(catchup.get_hash()?)?,
            }
        }
        notification::Which::CatchupComplete(complete) => {
            let complete = complete?;

            Notification::CatchupComplete {
                arena: parse_arena(complete.get_arena()?)?,
                index: complete.get_index(),
            }
        }
        notification::Which::Connected(connected) => {
            let connected = connected?;

            Notification::Connected {
                arena: parse_arena(connected.get_arena()?)?,
                uuid: parse_uuid(connected.get_uuid()?),
            }
        }
        notification::Which::Branch(branch) => {
            let branch = branch?;

            Notification::Branch {
                index: branch.get_index(),
                arena: parse_arena(branch.get_arena()?)?,
                source: parse_path(branch.get_source()?)?,
                dest: parse_path(branch.get_dest()?)?,
                hash: parse_hash(branch.get_hash()?)?,
            }
        }
        notification::Which::Rename(rename) => {
            let rename = rename?;

            Notification::Rename {
                index: rename.get_index(),
                arena: parse_arena(rename.get_arena()?)?,
                source: parse_path(rename.get_source()?)?,
                dest: parse_path(rename.get_dest()?)?,
                hash: parse_hash(rename.get_hash()?)?,
            }
        }
    })
}
