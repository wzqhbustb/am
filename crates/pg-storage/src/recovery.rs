//! Redo dispatch for crash recovery: [`RedoHandler`], [`RedoContext`], and
//! [`RedoRegistry`].
//!
//! Recovery replays WAL records through a registry that maps each
//! [`WalRecordType`] to exactly one handler. Two invariants come from the M2
//! tech-selection (§11.6, v2.3-24):
//!
//! - registering a second handler for the same record type **panics** — a
//!   duplicate registration is a programming error, not a runtime condition;
//! - applying a record whose type has no registered handler is a **hard
//!   failure** ([`StorageError::UnknownRecord`]) — recovery must never
//!   silently skip redo.
//!
//! M2 stages (heap, B+Tree, transactions) will register their own handlers
//! from their crates; `Engine::open` assembles them in one place (Stage F).

use std::collections::HashMap;
use std::fs::File;
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;

use parking_lot::Mutex;

use crate::buffer_pool::BufferPool;
use crate::clog::ClogAccessor;
use crate::error::{Result, StorageError};
use crate::page::set_page_pd_lsn;
use crate::page_allocator::PageAllocator;
use crate::types::{Lsn, PageId, TxnId, PAGE_SIZE};
use crate::wal::record::{bincode_config, FullPageImageRecord, WalRecord, WalRecordType};

/// Active transaction table (ARIES analysis).
///
/// Stage D provides the type so that [`RedoContext`] is complete; analysis
/// (rebuilding the ATT from the redo point) arrives in Stage N. Redo
/// handlers currently do not consult it.
#[derive(Debug, Default)]
pub struct ActiveXactTable {
    last_lsn: HashMap<TxnId, Lsn>,
}

impl ActiveXactTable {
    /// Create an empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the latest WAL LSN of an active transaction.
    pub fn add(&mut self, txn_id: TxnId, lsn: Lsn) {
        self.last_lsn.insert(txn_id, lsn);
    }

    /// Remove a transaction (on commit/abort).
    pub fn remove(&mut self, txn_id: TxnId) {
        self.last_lsn.remove(&txn_id);
    }

    /// Number of tracked transactions.
    pub fn len(&self) -> usize {
        self.last_lsn.len()
    }

    /// True if no transactions are tracked.
    pub fn is_empty(&self) -> bool {
        self.last_lsn.is_empty()
    }
}

/// Dirty page table (ARIES analysis).
///
/// Maps a dirty page to its recovery LSN (the oldest record that may need
/// redoing for it). Populated from Stage N; see [`ActiveXactTable`].
#[derive(Debug, Default)]
pub struct DirtyPageTable {
    rec_lsn: HashMap<PageId, Lsn>,
}

impl DirtyPageTable {
    /// Create an empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `page_id` is dirty as of `rec_lsn`.
    pub fn add(&mut self, page_id: PageId, rec_lsn: Lsn) {
        self.rec_lsn.insert(page_id, rec_lsn);
    }

    /// The recovery LSN of a page, if it is tracked as dirty.
    pub fn get(&self, page_id: PageId) -> Option<Lsn> {
        self.rec_lsn.get(&page_id).copied()
    }

    /// Number of tracked dirty pages.
    pub fn len(&self) -> usize {
        self.rec_lsn.len()
    }

    /// True if no dirty pages are tracked.
    pub fn is_empty(&self) -> bool {
        self.rec_lsn.is_empty()
    }
}

/// Context passed to every redo handler during replay.
///
/// All M2 stages compile against this struct from day one; the set of
/// collaborators a handler may touch is fixed here.
pub struct RedoContext<'a> {
    /// The buffer pool, if it exists at replay time.
    ///
    /// M1 recovery replays *before* opening the buffer pool (FPI images are
    /// written straight to the data file), so this is `None` there. AM redo
    /// handlers introduced from Stage I must tolerate `None` or run at a
    /// phase where the pool exists.
    pub buffer_pool: Option<&'a BufferPool>,
    /// The page allocator, for `PageAlloc` / `PageFree` redo.
    pub page_allocator: &'a Mutex<PageAllocator>,
    /// Commit-status accessor (M1: [`crate::clog::NoOpClogAccessor`]).
    pub clog: &'a dyn ClogAccessor,
    /// Active transaction table (Stage N).
    pub att: &'a mut ActiveXactTable,
    /// Dirty page table (Stage N).
    pub dpt: &'a mut DirtyPageTable,
}

/// Redo handler for one [`WalRecordType`].
pub trait RedoHandler: Send + Sync {
    /// The record type this handler applies.
    fn kind(&self) -> WalRecordType;
    /// Apply `record` to the on-disk/in-memory state.
    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()>;
}

/// Maps each [`WalRecordType`] to exactly one [`RedoHandler`].
#[derive(Default)]
pub struct RedoRegistry {
    handlers: HashMap<WalRecordType, Box<dyn RedoHandler>>,
}

impl RedoRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `handler`.
    ///
    /// # Panics
    ///
    /// Panics if a handler for the same record type is already registered:
    /// duplicate registration is a programming error (tech-selection
    /// v2.3-24), not a recoverable runtime condition.
    pub fn register(&mut self, handler: Box<dyn RedoHandler>) {
        let kind = handler.kind();
        if self.handlers.insert(kind, handler).is_some() {
            panic!("duplicate redo handler registered for {kind:?}");
        }
    }

    /// Dispatch `record` to its handler.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::UnknownRecord`] if no handler is registered
    /// for the record's type — recovery must never silently skip redo.
    pub fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let handler = self.handlers.get(&record.record_type).ok_or({
            StorageError::UnknownRecord {
                record_type: record.record_type.to_u8(),
                lsn: record.lsn,
            }
        })?;
        handler.apply(record, ctx)
    }
}

/// Redo handler for `PageAlloc` records: advances the page allocator past
/// the allocated page.
pub struct PageAllocRedoHandler;

impl RedoHandler for PageAllocRedoHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::PageAlloc
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        ctx.page_allocator.lock().replay_record(record)
    }
}

/// Redo handler for `PageFree` records: pushes the freed page back onto the
/// allocator's freelist so it can be reused after recovery.
pub struct PageFreeRedoHandler;

impl RedoHandler for PageFreeRedoHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::PageFree
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        ctx.page_allocator.lock().replay_record(record)
    }
}

/// Redo handler for `FullPageImage` records (M1 physical replay).
///
/// M1 replay writes images directly to the data file because the buffer pool
/// does not exist yet at replay time. After applying the image, the page's
/// `pd_lsn` is patched to the record's own LSN — matching PG, where the page
/// LSN becomes the LSN of the most recent redo record that touched it.
pub struct FullPageImageRedoHandler {
    data_file: Arc<Mutex<File>>,
}

impl FullPageImageRedoHandler {
    /// Create a handler that writes page images to `data_file`.
    pub fn new(data_file: Arc<Mutex<File>>) -> Self {
        Self { data_file }
    }
}

impl RedoHandler for FullPageImageRedoHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::FullPageImage
    }

    fn apply(&self, record: &WalRecord, _ctx: &mut RedoContext<'_>) -> Result<()> {
        let decoded: FullPageImageRecord =
            bincode::serde::decode_from_slice(&record.payload, bincode_config())
                .map_err(|e| StorageError::Serialize(e.to_string()))?
                .0;

        // M1 design note: replaying an FPI overwrites the page with the image
        // captured at the start of the checkpoint cycle. Any later in-place
        // modifications made *after* that FPI but *before* the next checkpoint
        // are lost on recovery because M1 has no redo records for heap/tuple
        // updates. This is acceptable for M1 (no Heap/BTree records); M2 will
        // replay fine-grained redo records after the FPI to reconstruct the
        // latest page state.
        let mut image = decoded.image;
        set_page_pd_lsn(&mut image, record.lsn);

        let offset = (decoded.page_id.0 - 1) * PAGE_SIZE as u64;
        let mut file = self.data_file.lock();
        file.seek(SeekFrom::Start(offset))
            .map_err(StorageError::Io)?;
        file.write_all(&image).map_err(StorageError::Io)?;
        Ok(())
    }
}

/// Redo handler for record types that carry no redo payload (currently the
/// checkpoint markers): replaying them is a no-op, but they must be
/// registered so that recovery does not fail them as unknown.
pub struct NoOpRedoHandler {
    kind: WalRecordType,
}

impl NoOpRedoHandler {
    /// Create a no-op handler for `kind`.
    pub fn new(kind: WalRecordType) -> Self {
        Self { kind }
    }
}

impl RedoHandler for NoOpRedoHandler {
    fn kind(&self) -> WalRecordType {
        self.kind
    }

    fn apply(&self, _record: &WalRecord, _ctx: &mut RedoContext<'_>) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clog::NoOpClogAccessor;
    use crate::config::StorageConfig;
    use crate::wal::writer::WalWriter;
    use tempfile::TempDir;

    struct TestComponents {
        _tmp: TempDir,
        allocator: Arc<Mutex<PageAllocator>>,
        _wal: Arc<WalWriter>,
    }

    fn setup() -> TestComponents {
        let tmp = TempDir::new().unwrap();
        let cfg = StorageConfig::new(tmp.path());
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let allocator = Arc::new(Mutex::new(
            PageAllocator::open(tmp.path(), &cfg, Arc::clone(&wal)).unwrap(),
        ));
        TestComponents {
            _tmp: tmp,
            allocator,
            _wal: wal,
        }
    }

    struct CountingHandler {
        kind: WalRecordType,
        count: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl RedoHandler for CountingHandler {
        fn kind(&self) -> WalRecordType {
            self.kind
        }

        fn apply(&self, _record: &WalRecord, _ctx: &mut RedoContext<'_>) -> Result<()> {
            self.count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(())
        }
    }

    fn noop_ctx<'a>(
        allocator: &'a Mutex<PageAllocator>,
        clog: &'a dyn ClogAccessor,
        att: &'a mut ActiveXactTable,
        dpt: &'a mut DirtyPageTable,
    ) -> RedoContext<'a> {
        RedoContext {
            buffer_pool: None,
            page_allocator: allocator,
            clog,
            att,
            dpt,
        }
    }

    #[test]
    fn registry_dispatches_to_registered_handler() {
        let c = setup();
        let clog = NoOpClogAccessor;
        let mut att = ActiveXactTable::new();
        let mut dpt = DirtyPageTable::new();

        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut registry = RedoRegistry::new();
        registry.register(Box::new(CountingHandler {
            kind: WalRecordType::HeapInsert,
            count: Arc::clone(&count),
        }));

        let record = WalRecord {
            lsn: Lsn(8),
            prev_lsn: Lsn::INVALID,
            txn_id: TxnId::INVALID,
            record_type: WalRecordType::HeapInsert,
            flags: 0,
            payload: Vec::new(),
        };
        let mut ctx = noop_ctx(&c.allocator, &clog, &mut att, &mut dpt);
        registry.apply(&record, &mut ctx).unwrap();
        assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    #[should_panic(expected = "duplicate redo handler")]
    fn duplicate_registration_panics() {
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut registry = RedoRegistry::new();
        registry.register(Box::new(CountingHandler {
            kind: WalRecordType::PageAlloc,
            count: Arc::clone(&count),
        }));
        registry.register(Box::new(CountingHandler {
            kind: WalRecordType::PageAlloc,
            count,
        }));
    }

    #[test]
    fn unregistered_record_is_hard_failure() {
        let c = setup();
        let clog = NoOpClogAccessor;
        let mut att = ActiveXactTable::new();
        let mut dpt = DirtyPageTable::new();

        let registry = RedoRegistry::new();
        let record = WalRecord {
            lsn: Lsn(8),
            prev_lsn: Lsn::INVALID,
            txn_id: TxnId::INVALID,
            record_type: WalRecordType::HeapInsert,
            flags: 0,
            payload: Vec::new(),
        };
        let mut ctx = noop_ctx(&c.allocator, &clog, &mut att, &mut dpt);
        let err = registry.apply(&record, &mut ctx).unwrap_err();
        assert!(matches!(
            err,
            StorageError::UnknownRecord {
                record_type: 1,
                lsn: Lsn(8)
            }
        ));
    }
}
