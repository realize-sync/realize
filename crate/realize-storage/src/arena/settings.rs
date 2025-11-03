#![allow(dead_code)] // WIP
use crate::arena::types::SettingsTableEntry;
use crate::config::DiskUsageConfig;
use crate::utils::holder::Holder;
use crate::{StorageError, arena::db::AfterCommit};
use redb::{ReadableTable, Table};
use tokio::sync::watch::{self, Ref};
use uuid::Uuid;

pub(crate) struct Settings {
    tx: watch::Sender<SettingsTableEntry>,
    rx: watch::Receiver<SettingsTableEntry>,
}

impl Settings {
    pub(crate) fn setup(
        table: &mut Table<'_, (), Holder<'static, SettingsTableEntry>>,
    ) -> Result<Self, StorageError> {
        let value = load(table)?;
        let (tx, rx) = watch::channel(value);

        Ok(Self { tx, rx })
    }

    pub(crate) fn borrow(&self) -> Ref<'_, SettingsTableEntry> {
        self.rx.borrow()
    }

    pub(crate) fn watch(&self) -> watch::Receiver<SettingsTableEntry> {
        self.tx.subscribe()
    }
}

pub(crate) struct WritableOpenSettings<'a> {
    table: Table<'a, (), Holder<'static, SettingsTableEntry>>,
    settings: &'a Settings,
    after_commit: &'a AfterCommit,
}

impl<'a> WritableOpenSettings<'a> {
    pub(crate) fn new(
        table: Table<'a, (), Holder<'static, SettingsTableEntry>>,
        settings: &'a Settings,
        after_commit: &'a AfterCommit,
    ) -> Self {
        Self {
            table,
            settings,
            after_commit,
        }
    }

    /// Load settings from the database.
    ///
    /// The settings returned by this call are guaranteed to match the transaction's,
    /// which isn't necessarily the case when using [Settings::borrow].
    pub(crate) fn load(&mut self) -> Result<SettingsTableEntry, StorageError> {
        load(&mut self.table)
    }

    /// Modify disk usage configuration in the settings.
    pub(crate) fn configure_disk_usage(
        &mut self,
        config: &DiskUsageConfig,
    ) -> Result<(), StorageError> {
        let mut settings = load(&mut self.table)?;
        settings.disk_usage = config.clone();
        self.table.insert((), Holder::new(&settings)?)?;
        let tx = self.settings.tx.clone();
        self.after_commit.add(move || {
            let _ = tx.send(settings);
        });

        Ok(())
    }
}

fn load(
    table: &mut Table<'_, (), Holder<'static, SettingsTableEntry>>,
) -> Result<SettingsTableEntry, StorageError> {
    if let Some(settings) = table.get(())? {
        Ok(settings.value().parse()?)
    } else {
        let settings = SettingsTableEntry {
            uuid: Uuid::now_v7(),
            disk_usage: DiskUsageConfig::default(),
        };
        table.insert((), Holder::new(&settings)?)?;

        Ok(settings)
    }
}
