//! Checkpoint coordinator.
//!
//! The checkpoint coordinator performs fuzzy checkpoints: it flushes dirty
//! buffer pool pages to disk while concurrent readers and writers continue to
//! access other pages. It writes `CheckpointBegin` / `CheckpointEnd` WAL records
//! and updates the superblock so that recovery knows where to start replay.
//!
//! Both manual checkpoints (`trigger_checkpoint`) and background periodic
//! checkpoints are supported. Automatic checkpoints run on a dedicated thread
//! until `shutdown` is called or the coordinator is dropped.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use parking_lot::Mutex;
use tracing::{debug, error, info, warn};

use crate::buffer_pool::BufferPool;
use crate::config::StorageConfig;
use crate::error::{Result, StorageError};
use crate::freelist_meta::FreelistMeta;
use crate::oid::OidCounter;
use crate::page_allocator::PageAllocator;
use crate::superblock::Superblock;
use crate::txn_id::TxnIdClock;
use crate::types::Lsn;
use crate::wal::record::WalRecord;
use crate::wal::writer::WalWriter;

/// Coordinates fuzzy checkpoints and persists the resulting anchor state.
#[derive(Debug)]
pub struct CheckpointCoordinator {
    data_dir: std::path::PathBuf,
    config: StorageConfig,
    superblock: Arc<Mutex<Superblock>>,
    buffer_pool: Arc<BufferPool>,
    page_allocator: Arc<Mutex<PageAllocator>>,
    wal_writer: Arc<WalWriter>,
    /// Serializes checkpoint execution so that manual and background checkpoints
    /// do not interleave.
    checkpoint_lock: Arc<Mutex<()>>,
    /// Source of the next OID to allocate, written into the v2 superblock
    /// on every checkpoint (Stage H). Initialized from the superblock's
    /// persisted `next_oid`, so checkpoints never roll the value back — not
    /// even to `Oid::FIRST_USER` — before the catalog wires its allocator
    /// via [`set_next_oid_source`](Self::set_next_oid_source).
    ///
    /// The counter holds the *next OID to hand out* (same semantics as
    /// `Superblock::next_oid`). Until the CheckpointEnd WAL record switches
    /// to v2 (Stage N), the superblock — not the WAL — is the authoritative
    /// source of `next_oid` across checkpoints.
    ///
    /// Wrapped in `Arc<Mutex<..>>` and shared with the background thread's
    /// clone so that a [`set_next_oid_source`](Self::set_next_oid_source)
    /// call made *after* the background checkpointer starts (the catalog
    /// wires its allocator only once `Catalog::open` runs, which may follow
    /// [`start_background_checkpointing`](Self::start_background_checkpointing))
    /// is still observed by background checkpoints.
    next_oid_source: Arc<Mutex<OidCounter>>,
    /// Source of the next transaction ID to allocate, written into the
    /// superblock on every checkpoint (Stage J). Mirrors `next_oid_source`:
    /// seeded from the superblock's persisted `next_txn_id`, replaced by the
    /// `TxnManager`'s clock via [`set_next_txn_id_source`](Self::set_next_txn_id_source),
    /// and read once per checkpoint. Persisting the live clock (instead of the
    /// old hardcoded `TxnId::FIRST`) lets recovery reseed the XID clock so
    /// post-restart transactions never reuse a committed XID.
    ///
    /// Shared with the background thread's clone for the same reason as
    /// `next_oid_source`.
    next_txn_id_source: Arc<Mutex<TxnIdClock>>,
    shutdown: Arc<AtomicBool>,
    background_handle: Mutex<Option<JoinHandle<()>>>,
}

impl CheckpointCoordinator {
    /// Create a new checkpoint coordinator.
    ///
    /// The caller must supply already-opened storage components. Background
    /// checkpointing is not started automatically; call
    /// [`start_background_checkpointing`](Self::start_background_checkpointing)
    /// to enable it.
    pub fn new(
        data_dir: impl Into<std::path::PathBuf>,
        config: &StorageConfig,
        superblock: Arc<Mutex<Superblock>>,
        buffer_pool: Arc<BufferPool>,
        page_allocator: Arc<Mutex<PageAllocator>>,
        wal_writer: Arc<WalWriter>,
    ) -> Self {
        // Seed the OID source from the persisted superblock value so every
        // checkpoint — including ones fired before the catalog wires its
        // allocator — persists a monotone `next_oid`.
        let next_oid = superblock.lock().next_oid;
        let next_txn_id = superblock.lock().next_txn_id;
        Self {
            data_dir: data_dir.into(),
            config: config.clone(),
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer,
            checkpoint_lock: Arc::new(Mutex::new(())),
            next_oid_source: Arc::new(Mutex::new(OidCounter::new(next_oid))),
            next_txn_id_source: Arc::new(Mutex::new(TxnIdClock::new(next_txn_id))),
            shutdown: Arc::new(AtomicBool::new(false)),
            background_handle: Mutex::new(None),
        }
    }

    /// Install the source of `next_oid` values persisted by checkpoints
    /// (Stage H wiring).
    ///
    /// `source` must hold the next OID to hand out; each
    /// [`trigger_checkpoint`](Self::trigger_checkpoint) reads it and writes the
    /// value into the v2 superblock. The coordinator starts with a counter
    /// seeded from the persisted superblock value; installing the catalog's
    /// allocator replaces it. The source is read once per checkpoint, so it
    /// may be replaced between checkpoints.
    ///
    /// The slot is shared (via `Arc`) with the background checkpoint thread's
    /// clone, so installing a source *after*
    /// [`start_background_checkpointing`](Self::start_background_checkpointing)
    /// still takes effect on the next background checkpoint.
    pub fn set_next_oid_source(&self, source: OidCounter) {
        *self.next_oid_source.lock() = source;
    }

    /// Install the source of `next_txn_id` values persisted by checkpoints
    /// (Stage J wiring).
    ///
    /// `source` is the `TxnManager`'s live [`TxnIdClock`]; each checkpoint
    /// reads its `current()` and writes it into the superblock's `next_txn_id`,
    /// so recovery reseeds the clock past every XID that could already be
    /// referenced by committed data. Mirrors
    /// [`set_next_oid_source`](Self::set_next_oid_source) — shared with the
    /// background thread, read once per checkpoint, replaceable between
    /// checkpoints.
    pub fn set_next_txn_id_source(&self, source: TxnIdClock) {
        *self.next_txn_id_source.lock() = source;
    }

    /// Start a background thread that triggers checkpoints periodically.
    ///
    /// If `config.checkpoint_interval_ms` is 0, this method returns immediately
    /// without starting a thread.
    ///
    /// # Errors
    ///
    /// Returns an error if background checkpointing is already running.
    pub fn start_background_checkpointing(&self) -> Result<()> {
        let interval_ms = self.config.checkpoint_interval_ms;
        if interval_ms == 0 {
            info!("automatic checkpoints are disabled (checkpoint_interval_ms=0)");
            return Ok(());
        }

        let mut handle = self.background_handle.lock();
        if handle.is_some() {
            return Err(StorageError::InvalidConfig(
                "background checkpointing already started".to_string(),
            ));
        }

        let shutdown = Arc::clone(&self.shutdown);
        let coordinator = self.clone_for_background_thread();
        let interval = Duration::from_millis(interval_ms);

        let join_handle = thread::Builder::new()
            .name("pg-checkpoint".to_string())
            .spawn(move || {
                info!(?interval, "background checkpoint thread started");
                loop {
                    // Sleep in small chunks so shutdown is responsive even with
                    // a long checkpoint interval.
                    const TICK_MS: u64 = 100;
                    let mut elapsed = 0u64;
                    while elapsed < interval_ms {
                        if shutdown.load(Ordering::Relaxed) {
                            debug!("background checkpoint thread shutting down");
                            return;
                        }
                        thread::sleep(Duration::from_millis(TICK_MS.min(interval_ms - elapsed)));
                        elapsed += TICK_MS.min(interval_ms - elapsed);
                    }

                    if shutdown.load(Ordering::Relaxed) {
                        debug!("background checkpoint thread shutting down");
                        return;
                    }

                    if let Err(e) = coordinator.trigger_checkpoint() {
                        // Background checkpoints log the error and keep going;
                        // the next checkpoint may succeed.
                        error!(error = %e, "background checkpoint failed");
                    }
                }
            })
            .map_err(StorageError::Io)?;

        *handle = Some(join_handle);
        Ok(())
    }

    /// Perform a checkpoint synchronously and return the `CheckpointBegin` LSN.
    ///
    /// The checkpoint is fuzzy: readers and writers on other pages proceed
    /// concurrently. Flushing an individual page briefly blocks writers on that
    /// page.
    ///
    /// # FPI race fix (Stage B)
    ///
    /// M1 had a race window between `wal_writer.append(begin_record)` returning
    /// and `set_checkpoint_lsn(begin_lsn)`: a `pin_mut` in that window could
    /// skip its FPI because `checkpoint_lsn` was still the old value. Stage B
    /// eliminates the window by pre-reserving the LSN and publishing it before
    /// emitting the record.
    ///
    /// # Freelist snapshot atomicity (Stage E)
    ///
    /// `reserve_lsn`, `set_checkpoint_lsn`, and `freelist.snapshot` are
    /// performed atomically under the `page_allocator` lock. This prevents a
    /// concurrent `free_page` from interleaving between the LSN reservation and
    /// the snapshot: such an interleaving would place the freed page in both
    /// the snapshot (in-memory push) and WAL replay (post-begin_lsn record),
    /// producing a duplicate freelist entry on recovery.
    pub fn trigger_checkpoint(&self) -> Result<Lsn> {
        // Serialize checkpoints so that manual and background checkpoints do not
        // interleave and produce redundant or overlapping checkpoint records.
        let _lock = self.checkpoint_lock.lock();

        // -- Phase 1: CheckpointBegin ----------------------------------------
        //
        // 1. Atomically reserve the checkpoint LSN, publish it to the buffer
        //    pool, and snapshot the freelist — all under the page_allocator
        //    lock. This is the core correctness invariant for concurrent
        //    free_page during a fuzzy checkpoint:
        //
        //    - A free_page that completes BEFORE this critical section has its
        //      page in the snapshot AND its WAL record at LSN < begin_lsn (not
        //      replayed). No duplicate.
        //    - A free_page that runs AFTER this critical section has its page
        //      NOT in the snapshot AND its WAL record at LSN > begin_lsn
        //      (replayed exactly once). No duplicate.
        //    - No free_page can run DURING this critical section (blocked on
        //      the page_allocator lock), so there is no window where a page
        //      is both in the snapshot and has a post-begin_lsn WAL record.
        //
        //    Lock order is page_allocator → WAL inner (reserve_lsn), which
        //    matches free_page's lock order — no deadlock.
        const CHECKPOINT_BEGIN_SIZE: u64 = crate::wal::record::WAL_RECORD_HEADER_SIZE as u64;
        let (begin_lsn, freelist_snap) = {
            let pa = self.page_allocator.lock();
            let begin_lsn = self.wal_writer.reserve_lsn(CHECKPOINT_BEGIN_SIZE)?;
            // Publish the checkpoint LSN immediately so any pin_mut from this
            // point on writes an FPI. Must be inside the lock to preserve the
            // Stage B FPI race fix (no window between reserve and publish).
            self.buffer_pool.set_checkpoint_lsn(begin_lsn);
            let snap = pa.snapshot(begin_lsn);
            (begin_lsn, snap)
        };
        debug!(%begin_lsn, "checkpoint begin (LSN reserved, freelist snapshot taken)");

        // 2. Emit the CheckpointBegin record into the reserved slot. This does
        //    not need the page_allocator lock — it only writes WAL.
        let begin_record = WalRecord::checkpoint_begin();
        self.wal_writer
            .append_at(begin_record, begin_lsn, CHECKPOINT_BEGIN_SIZE)?;
        debug!(%begin_lsn, "checkpoint begin record emitted");

        // -- Phase 2: Flush dirty pages --------------------------------------

        // 3. Collect dirty pages. This is a snapshot; pages may become clean or
        //    dirty while we iterate.
        let dirty_pages = self.buffer_pool.dirty_page_ids();
        debug!(count = dirty_pages.len(), "collected dirty pages");

        // 4. Flush each dirty page. flush() enforces WAL-before-data internally.
        //    M1 is conservative: any flush failure aborts the checkpoint so that
        //    the superblock is never updated to a checkpoint that did not
        //    actually complete.
        //
        //    PageNotFound is harmless: the page was evicted and flushed between
        //    dirty_page_ids() and our flush() call, so it is already durable.
        let mut flushed = 0usize;
        for page_id in dirty_pages {
            match self.buffer_pool.flush(page_id) {
                Ok(()) => flushed += 1,
                Err(StorageError::PageNotFound(_)) => {
                    debug!(%page_id, "page was evicted and flushed before checkpoint reached it");
                }
                Err(e) => {
                    return Err(StorageError::CheckpointFailed(format!(
                        "failed to flush page {page_id} during checkpoint: {e}"
                    )));
                }
            }
        }
        debug!(%flushed, "flushed dirty pages");

        // -- Phase 3: CheckpointEnd and superblock update ---------------------

        // 5. Capture allocator state for the checkpoint end record.
        let next_page_id = self.page_allocator.lock().next_page_id();
        let next_txn_id = self.next_txn_id_source.lock().current();

        // 6. Write CheckpointEnd. This marks the point at which the superblock
        //    can be safely updated.
        let end_record = WalRecord::checkpoint_end(begin_lsn, next_page_id, next_txn_id)?;
        let end_lsn = self.wal_writer.append(end_record)?;
        debug!(%end_lsn, "checkpoint end");

        // 7. Ensure CheckpointEnd and everything before it is fsynced before the
        //    freelist snapshot or superblock is written. (append() no longer
        //    fsyncs implicitly; this explicit flush_to is required.)
        self.wal_writer.flush_to(end_lsn)?;

        // 8. Write the freelist snapshot BEFORE the superblock. This is an
        //    acceleration hint for recovery: if present and valid, recovery
        //    seeds the allocator freelist from it and only replays
        //    post-checkpoint WAL records. If the snapshot is lost or
        //    corrupted, WAL replay rebuilds the freelist from scratch — so a
        //    failure here is non-fatal.
        //
        //    Ordering rationale: the snapshot must be written before the
        //    superblock so that a crash between the two leaves the superblock
        //    with the OLD checkpoint_lsn. Recovery then replays from the old
        //    LSN and sees all WAL records, correctly rebuilding the freelist.
        //    If the superblock were written first, a crash before the snapshot
        //    would leave recovery with the new LSN but a stale/missing
        //    snapshot, losing pre-checkpoint frees.
        if let Err(e) = freelist_snap.write(&FreelistMeta::path(&self.data_dir)) {
            warn!(error = %e, "failed to write freelist snapshot; recovery will rebuild from WAL");
        }

        // 9. Update the superblock. The redo LSN is the CheckpointBegin LSN.
        //    next_oid rides along in the v2 superblock: until the CheckpointEnd
        //    WAL record switches to v2 (Stage N), the superblock — not the WAL —
        //    is the authoritative source of next_oid across checkpoints. The
        //    source is always installed (seeded from the superblock at
        //    creation, replaced by the catalog's allocator later), so the
        //    persisted value is monotone.
        {
            let mut sb = self.superblock.lock();
            sb.checkpoint_lsn = begin_lsn;
            sb.next_page_id = next_page_id;
            sb.next_txn_id = next_txn_id;
            sb.next_oid = self.next_oid_source.lock().current();
            let sb_path = Superblock::path(&self.data_dir);
            sb.write(&sb_path)?;
        }
        info!(%begin_lsn, %flushed, "checkpoint completed");

        // 10. Recycle WAL segments that are no longer needed for recovery.
        self.wal_writer.recycle_before(begin_lsn)?;

        Ok(begin_lsn)
    }

    /// Stop the background checkpoint thread if it is running.
    ///
    /// This is called automatically on drop, but may be called explicitly to
    /// wait for the thread to finish (e.g., before tests assert on file state).
    pub fn shutdown(&self) {
        if self.shutdown.swap(true, Ordering::Relaxed) {
            return; // already shutting down
        }

        let mut handle = self.background_handle.lock();
        if let Some(h) = handle.take() {
            if let Err(e) = h.join() {
                warn!(error = ?e, "background checkpoint thread panicked");
            }
        }
    }

    /// Clone the state needed by the background checkpoint thread.
    fn clone_for_background_thread(&self) -> Self {
        Self {
            data_dir: self.data_dir.clone(),
            config: self.config.clone(),
            superblock: Arc::clone(&self.superblock),
            buffer_pool: Arc::clone(&self.buffer_pool),
            page_allocator: Arc::clone(&self.page_allocator),
            wal_writer: Arc::clone(&self.wal_writer),
            // Share the checkpoint lock with the foreground coordinator so that
            // manual and background checkpoints are mutually exclusive.
            checkpoint_lock: Arc::clone(&self.checkpoint_lock),
            // Share the next_oid source slot so a source installed after the
            // background thread starts (e.g. by Catalog::open) is still seen
            // by background checkpoints.
            next_oid_source: Arc::clone(&self.next_oid_source),
            // Share the next_txn_id source slot for the same reason as
            // next_oid_source: a source installed after the background thread
            // starts (by the TxnManager) must still be seen.
            next_txn_id_source: Arc::clone(&self.next_txn_id_source),
            shutdown: Arc::clone(&self.shutdown),
            background_handle: Mutex::new(None),
        }
    }
}

impl Drop for CheckpointCoordinator {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer_pool::BufferPool;
    use crate::config::StorageConfig;
    use crate::page::PAGE_HEADER_SIZE;
    use crate::page_allocator::PageAllocator;
    use crate::superblock::Superblock;
    use crate::types::Oid;
    use crate::wal::writer::WalWriter;
    use std::path::PathBuf;
    use tempfile::TempDir;

    type TestComponents = (
        PathBuf,
        StorageConfig,
        Arc<Mutex<Superblock>>,
        Arc<BufferPool>,
        Arc<Mutex<PageAllocator>>,
        Arc<WalWriter>,
    );

    fn setup(tmp: &TempDir) -> TestComponents {
        let data_dir = tmp.path().to_path_buf();
        let config = {
            let mut cfg = StorageConfig::new(&data_dir);
            cfg.buffer_pool_size = 8 * 1024 * 1024; // 8 MB => 1024 frames
            cfg
        };
        config.validate().unwrap();

        Superblock::create(&Superblock::path(&data_dir), config.page_size() as u32).unwrap();
        let superblock = Arc::new(Mutex::new(
            Superblock::read(&Superblock::path(&data_dir)).unwrap(),
        ));

        let wal_writer = Arc::new(WalWriter::open(&data_dir, &config).unwrap());
        let page_allocator = Arc::new(Mutex::new(
            PageAllocator::open(&data_dir, &config, Arc::clone(&wal_writer)).unwrap(),
        ));
        let buffer_pool = Arc::new(
            BufferPool::open(
                &data_dir,
                &config,
                Arc::clone(&page_allocator),
                Arc::clone(&wal_writer),
            )
            .unwrap(),
        );

        (
            data_dir,
            config,
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer,
        )
    }

    #[test]
    fn trigger_checkpoint_writes_begin_and_end() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        // Allocate and modify a page so there is dirty work to checkpoint.
        {
            let mut guard = buffer_pool.new_page().unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 42;
        }

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer.clone(),
        );

        let begin_lsn = coordinator.trigger_checkpoint().unwrap();
        assert!(begin_lsn.is_valid());

        // WAL should contain CheckpointBegin, FullPageImage (from new_page -> pin_mut),
        // and CheckpointEnd.
        let mut reader = crate::wal::reader::WalReader::open_at(
            data_dir.join("wal"),
            config.wal_segment_size,
            begin_lsn,
        )
        .unwrap();
        let mut saw_begin = false;
        let mut saw_end = false;
        while let Some(rec) = reader.next_record().unwrap() {
            match rec.record_type {
                crate::wal::record::WalRecordType::CheckpointBegin => saw_begin = true,
                crate::wal::record::WalRecordType::CheckpointEnd => {
                    let decoded: crate::wal::record::CheckpointEndRecord =
                        bincode::serde::decode_from_slice(
                            &rec.payload,
                            crate::wal::record::bincode_config(),
                        )
                        .unwrap()
                        .0;
                    assert_eq!(decoded.checkpoint_lsn, begin_lsn);
                    saw_end = true;
                }
                _ => {}
            }
        }
        assert!(saw_begin);
        assert!(saw_end);
    }

    #[test]
    fn checkpoint_updates_superblock() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        // Allocate a few pages.
        for _ in 0..5 {
            drop(buffer_pool.new_page().unwrap());
        }

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer,
        );

        let begin_lsn = coordinator.trigger_checkpoint().unwrap();

        let sb = Superblock::read(&Superblock::path(&data_dir)).unwrap();
        assert_eq!(sb.checkpoint_lsn, begin_lsn);
        assert!(sb.next_page_id.0 > 1);
        // Stage H: the OID source is seeded from the superblock at creation,
        // so on a fresh data directory checkpoints persist FIRST_USER — not
        // as a placeholder fallback, but because that is the persisted value.
        assert_eq!(sb.next_oid, crate::types::Oid::FIRST_USER);
    }

    #[test]
    fn checkpoint_persists_next_oid_from_source() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer,
        );

        // Stage H wiring: the catalog installs its OID allocator's counter as
        // the next_oid source; checkpoints persist its current value into the
        // v2 superblock.
        let source = OidCounter::new(Oid(20_000));
        coordinator.set_next_oid_source(source.clone());

        coordinator.trigger_checkpoint().unwrap();
        let sb = Superblock::read(&Superblock::path(&data_dir)).unwrap();
        assert_eq!(sb.next_oid, crate::types::Oid(20_000));

        // The source is read on every checkpoint, so allocations that advance
        // the counter are picked up by the next checkpoint.
        assert_eq!(source.alloc(), Oid(20_000));
        coordinator.trigger_checkpoint().unwrap();
        let sb = Superblock::read(&Superblock::path(&data_dir)).unwrap();
        assert_eq!(sb.next_oid, crate::types::Oid(20_001));
    }

    /// Regression: a next_oid source installed *after* the background thread's
    /// clone is created must still be observed by that clone. This guards the
    /// shared-`Arc` slot — the catalog wires its allocator in `Catalog::open`,
    /// which may run after `start_background_checkpointing`.
    #[test]
    fn background_clone_observes_source_installed_after_clone() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer,
        );

        // Clone for the background thread FIRST (source still the default
        // seeded from the superblock), then wire the source on the foreground
        // coordinator — the order a real startup hits when open() spawns the
        // checkpointer before Catalog::open.
        let background = coordinator.clone_for_background_thread();
        coordinator.set_next_oid_source(OidCounter::new(Oid(30_000)));

        // The background clone shares the slot, so its checkpoint persists the
        // late-installed source rather than the default it was cloned with.
        background.trigger_checkpoint().unwrap();
        let sb = Superblock::read(&Superblock::path(&data_dir)).unwrap();
        assert_eq!(sb.next_oid, crate::types::Oid(30_000));
    }

    #[test]
    fn background_checkpoint_runs_and_stops() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, mut config, superblock, buffer_pool, page_allocator, wal_writer) =
            setup(&tmp);
        // Use a short interval so the test does not take long.
        config.checkpoint_interval_ms = 50;

        // Allocate a few dirty pages so the background checkpoint has work to
        // flush and advances the superblock state.
        for _ in 0..3 {
            let mut guard = buffer_pool.new_page().unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0xCD;
        }

        let initial_lsn = superblock.lock().checkpoint_lsn;

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer,
        );

        coordinator.start_background_checkpointing().unwrap();

        // Wait long enough for at least one background checkpoint to fire.
        thread::sleep(Duration::from_millis(300));

        coordinator.shutdown();

        let sb = Superblock::read(&Superblock::path(&data_dir)).unwrap();
        assert!(sb.checkpoint_lsn.is_valid());
        assert!(
            sb.checkpoint_lsn > initial_lsn,
            "background checkpoint did not advance checkpoint_lsn"
        );
        assert!(sb.next_page_id.0 > 1);
    }

    #[test]
    fn checkpoint_lsn_controls_fpi_in_next_cycle() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        // Allocate and modify page 1.
        let page_id = {
            let mut guard = buffer_pool.new_page().unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 1;
            guard.page_id()
        };
        wal_writer.flush().unwrap();
        buffer_pool.flush(page_id).unwrap();

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool.clone(),
            page_allocator,
            wal_writer,
        );

        let first_lsn = coordinator.trigger_checkpoint().unwrap();

        // Evict the page and reload it. The next pin_mut should write an FPI
        // because the page was last modified before the checkpoint.
        let frame_count = buffer_pool.frame_count();
        for _ in 0..frame_count + 10 {
            drop(buffer_pool.new_page().unwrap());
        }

        let mut saw_fpi = false;
        {
            let mut guard = buffer_pool.pin_mut(page_id).unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 2;

            // Read the WAL from the first checkpoint begin to find the FPI.
            let mut reader = crate::wal::reader::WalReader::open_at(
                data_dir.join("wal"),
                config.wal_segment_size,
                first_lsn,
            )
            .unwrap();
            while let Some(rec) = reader.next_record().unwrap() {
                if rec.record_type == crate::wal::record::WalRecordType::FullPageImage {
                    let decoded: crate::wal::record::FullPageImageRecord =
                        bincode::serde::decode_from_slice(
                            &rec.payload,
                            crate::wal::record::bincode_config(),
                        )
                        .unwrap()
                        .0;
                    if decoded.page_id == page_id {
                        saw_fpi = true;
                    }
                }
            }
        }
        assert!(
            saw_fpi,
            "expected FPI after checkpoint for page modified before checkpoint"
        );
    }

    #[test]
    fn checkpoint_fpi_race_window_is_eliminated() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        // Allocate and dirty a page so the next checkpoint has work to do.
        let page_id = {
            let mut guard = buffer_pool.new_page().unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0xAA;
            guard.page_id()
        };

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool.clone(),
            page_allocator,
            wal_writer.clone(),
        );

        // Trigger checkpoint. With the Stage B fix, set_checkpoint_lsn is
        // called BEFORE the CheckpointBegin record is emitted, so any pin_mut
        // that starts after trigger_checkpoint returns will see the new LSN.
        let begin_lsn = coordinator.trigger_checkpoint().unwrap();

        // Invariant 1: buffer_pool's checkpoint_lsn must equal the reserved LSN.
        // We can't read it directly, but we can verify via behavior: a pin_mut
        // on a page modified before the checkpoint must write an FPI.
        //
        // Invariant 2: the CheckpointBegin record must exist at exactly
        // begin_lsn in the WAL.
        let mut reader = crate::wal::reader::WalReader::open_at(
            data_dir.join("wal"),
            config.wal_segment_size,
            begin_lsn,
        )
        .unwrap();
        let first_rec = reader
            .next_record()
            .unwrap()
            .expect("WAL must contain CheckpointBegin");
        assert_eq!(
            first_rec.record_type,
            crate::wal::record::WalRecordType::CheckpointBegin
        );
        assert_eq!(first_rec.lsn, begin_lsn);

        // Invariant 3: a subsequent pin_mut on a pre-checkpoint page must
        // produce an FPI because checkpoint_lsn was already visible.
        buffer_pool.flush(page_id).unwrap(); // ensure page is clean first
        let frame_count = buffer_pool.frame_count();
        for _ in 0..frame_count + 10 {
            drop(buffer_pool.new_page().unwrap());
        }

        let mut saw_fpi = false;
        {
            let mut guard = buffer_pool.pin_mut(page_id).unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0xBB;
            let mut reader2 = crate::wal::reader::WalReader::open_at(
                data_dir.join("wal"),
                config.wal_segment_size,
                begin_lsn,
            )
            .unwrap();
            while let Some(rec) = reader2.next_record().unwrap() {
                if rec.record_type == crate::wal::record::WalRecordType::FullPageImage {
                    let decoded: crate::wal::record::FullPageImageRecord =
                        bincode::serde::decode_from_slice(
                            &rec.payload,
                            crate::wal::record::bincode_config(),
                        )
                        .unwrap()
                        .0;
                    if decoded.page_id == page_id {
                        saw_fpi = true;
                    }
                }
            }
        }
        assert!(
            saw_fpi,
            "pin_mut after checkpoint must write FPI (checkpoint_lsn was visible)"
        );
    }

    #[test]
    fn checkpoint_succeeds_with_concurrent_wal_appends() {
        let tmp = TempDir::new().unwrap();
        let (data_dir, config, superblock, buffer_pool, page_allocator, wal_writer) = setup(&tmp);

        // Dirty a page so the checkpoint has flush work to do.
        {
            let mut guard = buffer_pool.new_page().unwrap();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0x5A;
        }

        let coordinator = CheckpointCoordinator::new(
            &data_dir,
            &config,
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer.clone(),
        );

        // Continuously append WAL records while checkpoints run. An append
        // landing between reserve_lsn(CheckpointBegin) and append_at advances
        // the clock past the reserved range; the relaxed append_at check must
        // tolerate that (it used to fail with "not at the reservation
        // boundary", failing the whole checkpoint).
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let wal = Arc::clone(&wal_writer);
        let appender = thread::spawn(move || {
            let mut count = 0u64;
            while !stop_flag.load(Ordering::Relaxed) {
                wal.append(WalRecord::page_alloc(crate::types::PageId(1_000_000 + count)).unwrap())
                    .unwrap();
                count += 1;
            }
            count
        });

        // Run many checkpoints so appends land inside the reserve → emit
        // window with high probability.
        for _ in 0..20 {
            coordinator.trigger_checkpoint().unwrap();
        }

        stop.store(true, Ordering::Relaxed);
        let appended = appender.join().unwrap();
        assert!(appended > 0, "appender thread made no progress");
    }
}
