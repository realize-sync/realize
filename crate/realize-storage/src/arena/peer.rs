use super::types::PeerTableEntry;
use crate::utils::holder::Holder;
use crate::{Progress, StorageError};
use realize_types::Peer;
use redb::{ReadableTable, Table};

pub(crate) struct ReadableOpenPeers<T, TN>
where
    T: ReadableTable<&'static str, Holder<'static, PeerTableEntry>>,
    TN: ReadableTable<&'static str, u64>,
{
    table: T,
    notification_table: TN,
}

impl<T, TN> ReadableOpenPeers<T, TN>
where
    T: ReadableTable<&'static str, Holder<'static, PeerTableEntry>>,
    TN: ReadableTable<&'static str, u64>,
{
    pub(crate) fn new(table: T, notification_table: TN) -> Self {
        Self {
            table,
            notification_table,
        }
    }
}

pub(crate) trait PeersReadOperations {
    fn progress(&self, peer: Peer) -> Result<Option<Progress>, StorageError>;
}

impl<T, TN> PeersReadOperations for ReadableOpenPeers<T, TN>
where
    T: ReadableTable<&'static str, Holder<'static, PeerTableEntry>>,
    TN: ReadableTable<&'static str, u64>,
{
    fn progress(&self, peer: Peer) -> Result<Option<Progress>, StorageError> {
        progress(&self.table, &self.notification_table, peer)
    }
}

impl<'a> PeersReadOperations for WritableOpenPeers<'a> {
    fn progress(&self, peer: Peer) -> Result<Option<Progress>, StorageError> {
        progress(&self.table, &self.notification_table, peer)
    }
}

pub(crate) struct WritableOpenPeers<'a> {
    table: Table<'a, &'static str, Holder<'static, PeerTableEntry>>,
    notification_table: Table<'a, &'static str, u64>,
}

impl<'a> WritableOpenPeers<'a> {
    pub(crate) fn new(
        table: Table<'a, &'static str, Holder<'static, PeerTableEntry>>,
        notification_table: Table<'a, &'static str, u64>,
    ) -> Self {
        Self {
            table,
            notification_table,
        }
    }

    pub(crate) fn update_last_seen_notification(
        &mut self,
        peer: Peer,
        index: u64,
    ) -> Result<(), StorageError> {
        self.notification_table.insert(peer.as_str(), index)?;

        Ok(())
    }

    pub(crate) fn connected(&mut self, peer: Peer, uuid: uuid::Uuid) -> Result<(), StorageError> {
        let key = peer.as_str();
        if let Some(entry) = self.table.get(key)? {
            if entry.value().parse()?.uuid == uuid {
                // We're connected to the same store as before; there's nothing to do.
                return Ok(());
            }
        }
        self.table
            .insert(key, Holder::with_content(PeerTableEntry { uuid })?)?;
        self.notification_table.remove(key)?;
        Ok(())
    }
}

fn progress(
    peer_table: &impl ReadableTable<&'static str, Holder<'static, PeerTableEntry>>,
    notification_table: &impl ReadableTable<&'static str, u64>,
    peer: Peer,
) -> Result<Option<Progress>, StorageError> {
    let key = peer.as_str();
    if let Some(entry) = peer_table.get(key)? {
        let PeerTableEntry { uuid, .. } = entry.value().parse()?;

        if let Some(last_seen) = notification_table.get(key)? {
            return Ok(Some(Progress::new(uuid, last_seen.value())));
        }
    }

    Ok(None)
}
