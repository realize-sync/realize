#![allow(dead_code)] // work in progress

use crate::StorageError;
use crate::utils::holder::{ByteConversionError, ByteConvertible, Holder, NamedType};
use capnp::message::ReaderOptions;
use capnp::serialize_packed;
use realize_types::Path;
use redb::{Database, TableDefinition};
use std::sync::Arc;
use tokio::task;

#[allow(dead_code)]
#[allow(unknown_lints)]
#[allow(clippy::uninlined_format_args)]
#[allow(clippy::extra_unused_type_parameters)]
mod engine_capnp {
    include!(concat!(env!("OUT_DIR"), "/engine_capnp.rs"));
}

/// Decision table for storing file decisions.
///
/// Key: &str (path)
/// Value: Holder<DecisionTableEntry>
const DECISION_TABLE: TableDefinition<&str, Holder<DecisionTableEntry>> =
    TableDefinition::new("engine.decision");

/// Engine for storing decisions, blocking version.
pub struct EngineBlocking {
    db: Arc<Database>,
}

impl EngineBlocking {
    pub fn new(db: Arc<Database>) -> Result<Self, StorageError> {
        // Initialize the database tables
        let txn = db.begin_write()?;
        txn.open_table(DECISION_TABLE)?;
        txn.commit()?;

        Ok(Self { db })
    }

    /// Turn this instance into an async one.
    pub fn into_async(self) -> EngineAsync {
        EngineAsync::new(self)
    }

    /// Set a decision for a path.
    pub fn set(&self, path: &Path, decision: Decision) -> Result<(), StorageError> {
        let txn = (&*self.db).begin_write()?;
        {
            let mut decision_table = txn.open_table(DECISION_TABLE)?;

            decision_table.insert(
                path.as_str(),
                Holder::with_content(DecisionTableEntry { decision })?,
            )?;
        }

        txn.commit()?;
        Ok(())
    }

    /// Clear a decision for a path.
    pub fn clear(&self, path: &Path) -> Result<(), StorageError> {
        let txn = (&*self.db).begin_write()?;
        {
            let mut decision_table = txn.open_table(DECISION_TABLE)?;

            decision_table.remove(path.as_str())?;
        }

        txn.commit()?;
        Ok(())
    }

    /// Get a decision for a path.
    pub fn get(&self, path: &Path) -> Result<Option<Decision>, StorageError> {
        let txn = (&*self.db).begin_read()?;
        let decision_table = txn.open_table(DECISION_TABLE)?;

        if let Some(entry) = decision_table.get(path.as_str())? {
            return Ok(Some(entry.value().parse()?.decision));
        }

        Ok(None)
    }
}

/// Engine for storing decisions, async version.
#[derive(Clone)]
pub struct EngineAsync {
    inner: Arc<EngineBlocking>,
}

impl EngineAsync {
    pub fn new(inner: EngineBlocking) -> Self {
        Self {
            inner: Arc::new(inner),
        }
    }

    /// Create an engine using the given database. Initialize the database if necessary.
    pub async fn with_db(db: Arc<redb::Database>) -> Result<Self, StorageError> {
        task::spawn_blocking(move || Ok(EngineAsync::new(EngineBlocking::new(db)?))).await?
    }

    /// Return a reference to the underlying blocking instance.
    pub fn blocking(&self) -> Arc<EngineBlocking> {
        Arc::clone(&self.inner)
    }

    /// Set a decision for a path.
    pub async fn set(&self, path: &Path, decision: Decision) -> Result<(), StorageError> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();

        task::spawn_blocking(move || inner.set(&path, decision)).await?
    }

    /// Clear a decision for a path.
    pub async fn clear(&self, path: &Path) -> Result<(), StorageError> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();

        task::spawn_blocking(move || inner.clear(&path)).await?
    }

    /// Get a decision for a path.
    pub async fn get(&self, path: &Path) -> Result<Option<Decision>, StorageError> {
        let inner = Arc::clone(&self.inner);
        let path = path.clone();

        task::spawn_blocking(move || inner.get(&path)).await?
    }
}

/// Decision types for file operations.
#[derive(Debug, Clone, PartialEq)]
pub enum Decision {
    /// Move from cache to arena root (as a regular file)
    Realize,
    /// Move from arena root to cache
    Unrealize,
    /// Update the file from another peer and store it in the cache
    UpdateCache,
}

/// Entry in the decision table.
#[derive(Debug, Clone, PartialEq)]
pub struct DecisionTableEntry {
    pub decision: Decision,
}

impl NamedType for DecisionTableEntry {
    fn typename() -> &'static str {
        "engine.decision"
    }
}

impl ByteConvertible<DecisionTableEntry> for DecisionTableEntry {
    fn from_bytes(data: &[u8]) -> Result<DecisionTableEntry, ByteConversionError> {
        let message_reader = serialize_packed::read_message(&mut &data[..], ReaderOptions::new())?;
        let msg: engine_capnp::decision_table_entry::Reader =
            message_reader.get_root::<engine_capnp::decision_table_entry::Reader>()?;

        let decision = match msg.get_decision()? {
            engine_capnp::Decision::Realize => Decision::Realize,
            engine_capnp::Decision::Unrealize => Decision::Unrealize,
            engine_capnp::Decision::UpdateCache => Decision::UpdateCache,
        };

        Ok(DecisionTableEntry { decision })
    }

    fn to_bytes(&self) -> Result<Vec<u8>, ByteConversionError> {
        let mut message = ::capnp::message::Builder::new_default();
        let mut builder: engine_capnp::decision_table_entry::Builder =
            message.init_root::<engine_capnp::decision_table_entry::Builder>();

        let decision = match self.decision {
            Decision::Realize => engine_capnp::Decision::Realize,
            Decision::Unrealize => engine_capnp::Decision::Unrealize,
            Decision::UpdateCache => engine_capnp::Decision::UpdateCache,
        };
        builder.set_decision(decision);

        let mut buffer: Vec<u8> = Vec::new();
        serialize_packed::write_message(&mut buffer, &message)?;

        Ok(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::redb_utils;

    struct Fixture {
        engine: EngineBlocking,
    }
    impl Fixture {
        fn setup() -> anyhow::Result<Fixture> {
            let engine = EngineBlocking::new(redb_utils::in_memory()?)?;
            Ok(Self { engine })
        }
    }

    #[test]
    fn convert_decision_table_entry() -> anyhow::Result<()> {
        let entry = DecisionTableEntry {
            decision: Decision::Realize,
        };

        assert_eq!(
            entry,
            DecisionTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        let entry = DecisionTableEntry {
            decision: Decision::Unrealize,
        };

        assert_eq!(
            entry,
            DecisionTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        let entry = DecisionTableEntry {
            decision: Decision::UpdateCache,
        };

        assert_eq!(
            entry,
            DecisionTableEntry::from_bytes(entry.clone().to_bytes()?.as_slice())?
        );

        Ok(())
    }

    #[test]
    fn engine_set_and_get() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let engine = &fixture.engine;
        let path = Path::parse("foo/bar.txt")?;

        // Initially no decision
        assert_eq!(None, engine.get(&path)?);

        // Set a decision
        engine.set(&path, Decision::Realize)?;
        assert_eq!(Some(Decision::Realize), engine.get(&path)?);

        // Update the decision
        engine.set(&path, Decision::Unrealize)?;
        assert_eq!(Some(Decision::Unrealize), engine.get(&path)?);

        Ok(())
    }

    #[test]
    fn engine_clear() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let engine = &fixture.engine;
        let path = Path::parse("foo/bar.txt")?;

        // Set a decision
        engine.set(&path, Decision::UpdateCache)?;
        assert_eq!(Some(Decision::UpdateCache), engine.get(&path)?);

        // Clear the decision
        engine.clear(&path)?;
        assert_eq!(None, engine.get(&path)?);

        // Clearing a non-existent decision should not error
        engine.clear(&path)?;
        assert_eq!(None, engine.get(&path)?);

        Ok(())
    }

    #[test]
    fn engine_multiple_paths() -> anyhow::Result<()> {
        let fixture = Fixture::setup()?;
        let engine = &fixture.engine;
        let path1 = Path::parse("foo/bar.txt")?;
        let path2 = Path::parse("baz/qux.txt")?;

        // Set different decisions for different paths
        engine.set(&path1, Decision::Realize)?;
        engine.set(&path2, Decision::Unrealize)?;

        assert_eq!(Some(Decision::Realize), engine.get(&path1)?);
        assert_eq!(Some(Decision::Unrealize), engine.get(&path2)?);

        // Clear one path, the other should remain
        engine.clear(&path1)?;
        assert_eq!(None, engine.get(&path1)?);
        assert_eq!(Some(Decision::Unrealize), engine.get(&path2)?);

        Ok(())
    }
}
