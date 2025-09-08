use crate::arena::db::ArenaDatabase;
use crate::arena::index::{self, IndexExt};
use crate::{Notification, StorageError};
use realize_types::Peer;
use std::sync::Arc;

/// Apply `notification` to the given database.
///
/// The notification must be meant for this arena.
pub(crate) fn apply(
    db: &Arc<ArenaDatabase>,
    index_root: Option<&std::path::Path>,
    peer: Peer,
    notification: Notification,
) -> Result<(), StorageError> {
    let arena = db.arena();
    log::trace!("[{arena}]@{peer} Notification: {notification:?}",);
    // UnrealCache::update, is responsible for dispatching properly
    assert_eq!(arena, notification.arena());

    let txn = db.begin_write()?;
    {
        let mut tree = txn.write_tree()?;
        let mut peers = txn.write_peers()?;
        let mut cache = txn.write_cache()?;
        let mut blobs = txn.write_blobs()?;
        let mut dirty = txn.write_dirty()?;
        if let Some(index) = notification.index() {
            peers.update_last_seen_notification(peer, index)?;
        }
        match notification {
            Notification::Add {
                path,
                mtime,
                size,
                hash,
                ..
            } => cache.notify_added(
                &mut tree, &mut blobs, &mut dirty, peer, path, mtime, size, hash,
            )?,
            Notification::Replace {
                path,
                mtime,
                size,
                hash,
                old_hash,
                ..
            } => {
                cache.notify_replaced(
                    &mut tree, &mut blobs, &mut dirty, peer, &path, mtime, size, &hash, &old_hash,
                )?;

                let mut index = txn.write_index()?;
                index.record_outdated(&tree, &path, &old_hash, &hash)?;
            }

            Notification::Remove { path, old_hash, .. } => {
                cache.notify_dropped_or_removed(
                    &mut tree, &mut blobs, &mut dirty, peer, &path, &old_hash, false,
                )?;

                if let Some(index_root) = index_root {
                    if let Some(indexed) = txn.read_index()?.get(&tree, &path)?
                        && indexed.is_outdated_by(&old_hash)
                        && indexed.matches_file(path.within(&index_root))
                    {
                        // This specific version has been removed
                        // remotely. Make sure that the file hasn't
                        // changed since it was indexed and if it hasn't,
                        // remove it locally as well.
                        std::fs::remove_file(&path.within(index_root))?;
                    }
                }
            }
            Notification::Drop { path, old_hash, .. } => cache.notify_dropped_or_removed(
                &mut tree, &mut blobs, &mut dirty, peer, path, &old_hash, true,
            )?,
            Notification::CatchupStart(_) => cache.catchup_start(peer)?,
            Notification::Catchup {
                path,
                mtime,
                size,
                hash,
                ..
            } => cache.catchup(
                &mut tree, &mut blobs, &mut dirty, peer, path, mtime, size, hash,
            )?,
            Notification::CatchupComplete { .. } => {
                cache.catchup_complete(&mut tree, &mut blobs, &mut dirty, peer)?
            }
            Notification::Connected { uuid, .. } => peers.connected(peer, uuid)?,
            Notification::Branch {
                source,
                dest,
                hash,
                old_hash,
                ..
            } => {
                if let Some(index_root) = index_root {
                    let index = txn.read_index()?;
                    if index::branch(
                        &index,
                        &tree,
                        index_root,
                        &source,
                        &dest,
                        &hash,
                        old_hash.as_ref(),
                    )? {
                        log::debug!("[{arena}] branched {source} {hash} -> {dest}");
                    } else {
                        log::debug!(
                            "[{arena}] wrong versions; ignored:
  branch {source} {hash} -> {dest} {old_hash:?}"
                        )
                    }
                }
            }
            Notification::Rename {
                source,
                dest,
                hash,
                old_hash,
                ..
            } => {
                if let Some(index_root) = index_root {
                    let mut index = txn.write_index()?;
                    let mut history = txn.write_history()?;
                    if index::rename(
                        &mut index,
                        &mut tree,
                        &mut history,
                        &mut dirty,
                        index_root,
                        &source,
                        &dest,
                        &hash,
                        old_hash.as_ref(),
                    )? {
                        log::debug!("[{arena}] renamed {source} {hash} -> {dest}");
                    } else {
                        log::debug!(
                            "[{arena}] wrong versions; ignored:
  rename {source} {hash} -> {dest} {old_hash:?}"
                        )
                    }
                }
            }
        }
    }
    txn.commit()?;
    Ok(())
}
