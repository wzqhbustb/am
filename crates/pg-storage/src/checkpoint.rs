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
use crate::page_allocator::PageAllocator;
use crate::superblock::Superblock;
use crate::types::{Lsn, TxnId};
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
        Self {
            data_dir: data_dir.into(),
            config: config.clone(),
            superblock,
            buffer_pool,
            page_allocator,
            wal_writer,
            checkpoint_lock: Arc::new(Mutex::new(())),
            shutdown: Arc::new(AtomicBool::new(false)),
            background_handle: Mutex::new(None),
        }
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
    /// eliminates the window by pre-reserving the LSN:
    ///
    /// 1. `reserve_lsn(CHECKPOINT_BEGIN_SIZE)` — advance the clock and return
    ///    the future LSN without writing anything.
    /// 2. `set_checkpoint_lsn(begin_lsn)` — publish the new checkpoint LSN
    ///    immediately; any `pin_mut` from this point on will write an FPI.
    /// 3. `append_at(begin_record, begin_lsn)` — emit the `CheckpointBegin`
    ///    record into the already-reserved slot.
    ///
    /// Because step 2 happens before step 3, there is no window in which a
    /// page modification can miss its FPI.
    pub fn trigger_checkpoint(&self) -> Result<Lsn> {
        // Serialize checkpoints so that manual and background checkpoints do not
        // interleave and produce redundant or overlapping checkpoint records.
        let _lock = self.checkpoint_lock.lock();

        // -- Phase 1: CheckpointBegin ----------------------------------------

        // 1. Pre-reserve the CheckpointBegin LSN. A CheckpointBegin record has
        //    an empty payload, so its total size is exactly WAL_RECORD_HEADER_SIZE
        //    (32 bytes), already aligned to LSN_ALIGNMENT.
        const CHECKPOINT_BEGIN_SIZE: u64 = crate::wal::record::WAL_RECORD_HEADER_SIZE as u64;
        let begin_lsn = self.wal_writer.reserve_lsn(CHECKPOINT_BEGIN_SIZE)?;

        // 2. Publish the checkpoint LSN immediately. From this point on, the
        //    first mutable access to any page in this checkpoint cycle will
        //    write an FPI.
        self.buffer_pool.set_checkpoint_lsn(begin_lsn);
        debug!(%begin_lsn, "checkpoint begin (LSN pre-reserved)");

        // 3. Emit the CheckpointBegin record into the reserved slot.
        let begin_record = WalRecord::checkpoint_begin();
        self.wal_writer
            .append_at(begin_record, begin_lsn, CHECKPOINT_BEGIN_SIZE)?;
        debug!(%begin_lsn, "checkpoint begin record emitted");

        // -- Phase 2: Flush dirty pages --------------------------------------

        // 4. Collect dirty pages. This is a snapshot; pages may become clean or
        //    dirty while we iterate.
        let dirty_pages = self.buffer_pool.dirty_page_ids();
        debug!(count = dirty_pages.len(), "collected dirty pages");

        // 5. Flush each dirty page. flush() enforces WAL-before-data internally.
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

        // 6. Capture allocator state for the checkpoint end record.
        let next_page_id = self.page_allocator.lock().next_page_id();
        let next_txn_id = TxnId::FIRST; // M1 has no transactions.

        // 7. Write CheckpointEnd. This marks the point at which the superblock
        //    can be safely updated.
        let end_record = WalRecord::checkpoint_end(begin_lsn, next_page_id, next_txn_id)?;
        let end_lsn = self.wal_writer.append(end_record)?;
        debug!(%end_lsn, "checkpoint end");

        // 8. Ensure CheckpointEnd and everything before it is fsynced before the
        //    superblock is updated or old WAL segments are recycled. (append()
        //    no longer fsyncs implicitly; this explicit flush_to is required.)
        self.wal_writer.flush_to(end_lsn)?;

        // 9. Update the superblock. The redo LSN is the CheckpointBegin LSN.
        //    next_oid rides along in the v2 superblock: until the CheckpointEnd
        //    WAL record switches to v2 (Stage N), the superblock — not the WAL —
        //    is the authoritative source of next_oid across checkpoints.
        {
            let mut sb = self.superblock.lock();
            sb.checkpoint_lsn = begin_lsn;
            sb.next_page_id = next_page_id;
            sb.next_txn_id = next_txn_id;
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
    use crate::page_allocator::PageAllocator;
    use crate::superblock::Superblock;
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
            guard.page_mut()[0] = 42;
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
        // Stage C: next_oid is persisted via the v2 superblock on every
        // checkpoint (nothing allocates OIDs yet, so it stays FIRST_USER).
        assert_eq!(sb.next_oid, crate::types::Oid::FIRST_USER);
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
            guard.page_mut()[0] = 0xCD;
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
            guard.page_mut()[0] = 1;
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
            guard.page_mut()[0] = 2;

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
            guard.page_mut()[0] = 0xAA;
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
            guard.page_mut()[0] = 0xBB;
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
            guard.page_mut()[0] = 0x5A;
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
