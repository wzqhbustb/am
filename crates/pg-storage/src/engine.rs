//! Top-level storage engine.
//!
//! `StorageEngine` owns and wires together all M1 storage components:
//! superblock, page allocator, WAL writer, buffer pool, and checkpoint
//! coordinator. It also provides the canonical crash-recovery entry point
//! [`StorageEngine::recover`].

use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use tracing::{info, warn};

use crate::buffer_pool::BufferPool;
use crate::checkpoint::CheckpointCoordinator;
use crate::config::StorageConfig;
use crate::error::{Result, StorageError};
use crate::freelist_meta::FreelistMeta;
use crate::io::ensure_data_dir;
use crate::page_allocator::PageAllocator;
use crate::superblock::Superblock;
use crate::types::{Lsn, PageId};
use crate::wal::reader::WalReader;
use crate::wal::record::WalRecord;
use crate::wal::writer::WalWriter;

/// Owning handle for a recovered or newly created storage engine.
#[derive(Debug)]
pub struct StorageEngine {
    data_dir: PathBuf,
    config: StorageConfig,
    superblock: Arc<Mutex<Superblock>>,
    page_allocator: Arc<Mutex<PageAllocator>>,
    wal_writer: Arc<WalWriter>,
    buffer_pool: Arc<BufferPool>,
    checkpoint: CheckpointCoordinator,
}

impl StorageEngine {
    /// Open or create a storage engine at `data_dir`.
    ///
    /// If a superblock already exists, this calls [`Self::recover`]. Otherwise
    /// it initializes a fresh database and returns an engine ready for use.
    ///
    /// Background checkpointing is not started automatically; call
    /// [`Self::start_background_checkpointing`] to enable it.
    pub fn open(data_dir: impl AsRef<Path>, config: &StorageConfig) -> Result<Self> {
        config.validate()?;
        let data_dir = data_dir.as_ref().to_path_buf();
        ensure_data_dir(&data_dir)?;

        let sb_path = Superblock::path(&data_dir);
        if sb_path.exists() {
            Self::recover(data_dir, config)
        } else {
            Self::create_new(data_dir, config)
        }
    }

    /// Create a brand-new database.
    fn create_new(data_dir: PathBuf, config: &StorageConfig) -> Result<Self> {
        info!(data_dir = %data_dir.display(), "creating new storage engine");

        let sb_path = Superblock::path(&data_dir);
        let superblock = Superblock::create(&sb_path, config.page_size() as u32)?;
        let superblock = Arc::new(Mutex::new(superblock));

        let wal_writer = Arc::new(WalWriter::open(&data_dir, config)?);
        let page_allocator = Arc::new(Mutex::new(PageAllocator::open(
            &data_dir,
            config,
            Arc::clone(&wal_writer),
        )?));
        let buffer_pool = Arc::new(BufferPool::open(
            &data_dir,
            config,
            Arc::clone(&page_allocator),
            Arc::clone(&wal_writer),
        )?);

        // M1 has no persistent freelist state to load; this is a no-op that
        // creates the file if it does not exist.
        let _ = FreelistMeta::read_or_default(&data_dir)?;

        let checkpoint = CheckpointCoordinator::new(
            &data_dir,
            config,
            Arc::clone(&superblock),
            Arc::clone(&buffer_pool),
            Arc::clone(&page_allocator),
            Arc::clone(&wal_writer),
        );

        Ok(Self {
            data_dir,
            config: config.clone(),
            superblock,
            page_allocator,
            wal_writer,
            buffer_pool,
            checkpoint,
        })
    }

    /// Recover a storage engine from disk after a crash or clean shutdown.
    ///
    /// Recovery follows the M1 procedure from `docs/phase1-m1-tech-selection.md`
    /// §十一:
    ///
    /// 1. Read the superblock to obtain the redo point (`checkpoint_lsn`).
    /// 2. Initialize the page allocator with the checkpointed `next_page_id`.
    /// 3. Load the freelist snapshot (M1: always empty).
    /// 4. Replay WAL from `checkpoint_lsn`:
    ///    - `PageAlloc` advances `next_page_id`.
    ///    - `FullPageImage` overwrites the data file page, repairing torn writes.
    ///    - `CheckpointBegin` / `CheckpointEnd` are ignored (anchor state is
    ///      already in the superblock).
    ///    - A truncated or CRC-bad final record is treated as end-of-WAL.
    /// 5. Open the WAL writer and buffer pool at the recovered state.
    pub fn recover(data_dir: PathBuf, config: &StorageConfig) -> Result<Self> {
        info!(data_dir = %data_dir.display(), "recovering storage engine");

        let sb_path = Superblock::path(&data_dir);
        let superblock = Superblock::read(&sb_path)?;
        let checkpoint_lsn = superblock.checkpoint_lsn;
        info!(%checkpoint_lsn, "loaded superblock");

        // 2. Initialize the page allocator from the checkpointed next_page_id.
        //    We need a temporary WAL writer for replay; it will be replaced by
        //    the real one after recovery.
        //
        // TODO(M2): avoid creating a throwaway WalWriter. The allocator only
        // needs a writer for future allocations; replay itself does not write WAL.
        let replay_wal = Arc::new(WalWriter::open(&data_dir, config)?);
        let page_allocator = Arc::new(Mutex::new(PageAllocator::open_at(
            &data_dir,
            config,
            Arc::clone(&replay_wal),
            superblock.next_page_id,
        )?));

        // 3. Load freelist snapshot. Corruption or absence is harmless because
        //    the WAL replay below rebuilds the allocator state.
        let _ = FreelistMeta::read_or_default(&data_dir);

        // 4. Replay WAL from the checkpoint redo point. If no checkpoint has
        //    ever run, start from Lsn::FIRST so that all WAL records written
        //    before the first checkpoint are replayed.
        let replay_start = if checkpoint_lsn.is_valid() {
            checkpoint_lsn
        } else {
            warn!("checkpoint_lsn is invalid; replaying WAL from the beginning");
            Lsn::FIRST
        };
        Self::replay_wal(
            data_dir.clone(),
            config,
            replay_start,
            Arc::clone(&page_allocator),
        )?;
        page_allocator.lock().mark_recovery_complete();

        // 5. Open WAL writer and buffer pool at the recovered state.
        //    WalWriter::open scans the durable WAL and resumes appending from the
        //    byte position immediately after the last complete record, so the new
        //    writer is consistent with the file state left by replay (and by the
        //    temporary replay writer, which did not write any records).
        let wal_writer = Arc::new(WalWriter::open(&data_dir, config)?);
        // Replace the temporary replay writer with the real one so that future
        // allocations go through the same WAL writer as the buffer pool.
        page_allocator
            .lock()
            .set_wal_writer(Arc::clone(&wal_writer));
        let buffer_pool = Arc::new(BufferPool::open(
            &data_dir,
            config,
            Arc::clone(&page_allocator),
            Arc::clone(&wal_writer),
        )?);

        let superblock = Arc::new(Mutex::new(superblock));
        let checkpoint = CheckpointCoordinator::new(
            &data_dir,
            config,
            Arc::clone(&superblock),
            Arc::clone(&buffer_pool),
            Arc::clone(&page_allocator),
            Arc::clone(&wal_writer),
        );

        info!("recovery complete");
        Ok(Self {
            data_dir,
            config: config.clone(),
            superblock,
            page_allocator,
            wal_writer,
            buffer_pool,
            checkpoint,
        })
    }

    fn replay_wal(
        data_dir: PathBuf,
        config: &StorageConfig,
        checkpoint_lsn: Lsn,
        page_allocator: Arc<Mutex<PageAllocator>>,
    ) -> Result<()> {
        let mut reader = WalReader::open_at(
            data_dir.join("wal"),
            config.wal_segment_size,
            checkpoint_lsn,
        )?;

        // Open the data file once and reuse it for all FullPageImage records.
        let data_file_path = data_dir.join("data").join("datafile");
        let mut data_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .open(&data_file_path)
            .map_err(StorageError::Io)?;

        let mut records_replayed = 0usize;

        loop {
            match reader.next_record() {
                Ok(Some(record)) => {
                    Self::apply_record(&record, &mut data_file, Arc::clone(&page_allocator))?;
                    records_replayed += 1;
                }
                Ok(None) => break,
                Err(e) => {
                    // A bad final record is treated as end-of-WAL; anything
                    // before it has already been applied.
                    warn!(error = %e, "WAL replay stopped at truncated/final record");
                    break;
                }
            }
        }

        // Ensure all replayed FPIs are durable before returning.
        data_file.sync_all().map_err(StorageError::Io)?;

        info!(records_replayed, "WAL replay complete");
        Ok(())
    }

    fn apply_record(
        record: &WalRecord,
        data_file: &mut std::fs::File,
        page_allocator: Arc<Mutex<PageAllocator>>,
    ) -> Result<()> {
        use crate::wal::record::WalRecordType;
        match record.record_type {
            WalRecordType::PageAlloc => {
                page_allocator.lock().replay_record(record)?;
            }
            WalRecordType::FullPageImage => {
                let decoded: crate::wal::record::FullPageImageRecord =
                    bincode::serde::decode_from_slice(
                        &record.payload,
                        crate::wal::record::bincode_config(),
                    )
                    .map_err(|e| StorageError::Serialize(e.to_string()))?
                    .0;
                // M1 design note: replaying an FPI overwrites the page with the
                // image captured at the start of the checkpoint cycle. Any later
                // in-place modifications made *after* that FPI but *before* the
                // next checkpoint are lost on recovery because M1 has no redo
                // records for heap/tuple updates. This is acceptable for M1
                // (no Heap/BTree records); M2 will replay fine-grained redo
                // records after the FPI to reconstruct the latest page state.
                Self::write_page_image_to_data_file(data_file, &decoded.page_id, &decoded.image)?;
            }
            _ => {
                // M1 only handles PageAlloc and FullPageImage replay. All other
                // record types (Heap*, BTree*, Txn*, PageFree, Logical*,
                // Segment*) are ignored here; they will be implemented in later
                // phases.
            }
        }
        Ok(())
    }

    fn write_page_image_to_data_file(
        data_file: &mut std::fs::File,
        page_id: &PageId,
        image: &[u8],
    ) -> Result<()> {
        use std::io::{Seek, SeekFrom, Write};

        let offset = (page_id.0 - 1) * crate::types::PAGE_SIZE as u64;
        data_file
            .seek(SeekFrom::Start(offset))
            .map_err(StorageError::Io)?;
        data_file.write_all(image).map_err(StorageError::Io)?;
        Ok(())
    }

    /// Return the data directory.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Return the storage configuration.
    pub fn config(&self) -> &StorageConfig {
        &self.config
    }

    /// Return a reference to the superblock.
    pub fn superblock(&self) -> &Arc<Mutex<Superblock>> {
        &self.superblock
    }

    /// Return a reference to the buffer pool.
    pub fn buffer_pool(&self) -> &Arc<BufferPool> {
        &self.buffer_pool
    }

    /// Return a reference to the page allocator.
    pub fn page_allocator(&self) -> &Arc<Mutex<PageAllocator>> {
        &self.page_allocator
    }

    /// Return a reference to the WAL writer.
    pub fn wal_writer(&self) -> &Arc<WalWriter> {
        &self.wal_writer
    }

    /// Return a reference to the checkpoint coordinator.
    pub fn checkpoint(&self) -> &CheckpointCoordinator {
        &self.checkpoint
    }

    /// Manually trigger a checkpoint.
    pub fn trigger_checkpoint(&self) -> Result<Lsn> {
        self.checkpoint.trigger_checkpoint()
    }

    /// Start automatic background checkpoints.
    pub fn start_background_checkpointing(&self) -> Result<()> {
        self.checkpoint.start_background_checkpointing()
    }

    /// Gracefully shut down background threads.
    pub fn shutdown(&self) {
        self.checkpoint.shutdown();
        // WalWriter's Drop handles its own worker shutdown.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_recover_empty_engine() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());

        {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            engine.trigger_checkpoint().unwrap();
        }

        {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            assert!(engine.superblock.lock().checkpoint_lsn.is_valid());
        }
    }

    #[test]
    fn write_and_recover_data_after_checkpoint() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());

        let page_id = {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            let mut guard = engine.buffer_pool().new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[0..4].copy_from_slice(&[1, 2, 3, 4]);
            drop(guard);
            engine.trigger_checkpoint().unwrap();
            id
        };

        {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            let guard = engine.buffer_pool().pin(page_id).unwrap();
            assert_eq!(&guard.page()[0..4], &[1, 2, 3, 4]);
        }
    }

    #[test]
    fn recover_without_checkpoint_replays_page_allocs() {
        let tmp = tempfile::TempDir::new().unwrap();
        let config = StorageConfig::new(tmp.path());

        let page_id = {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            let guard = engine.buffer_pool().new_page().unwrap();
            let id = guard.page_id();
            drop(guard);
            // Intentionally do NOT checkpoint. The next open must replay the
            // PageAlloc WAL record so that it does not hand out the same id.
            id
        };

        {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            let guard = engine.buffer_pool().new_page().unwrap();
            assert_ne!(guard.page_id(), page_id, "PageAlloc WAL was not replayed");
            assert!(guard.page_id().0 > page_id.0);
        }
    }

    #[test]
    fn checkpoint_recycles_old_wal_segments() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = StorageConfig::new(tmp.path());
        // Use a tiny segment size so that a modest number of allocations spans
        // several segments and checkpointing has something to recycle.
        config.wal_segment_size = 1024;

        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        // Allocate and modify enough pages to span multiple WAL segments.
        for _ in 0..64 {
            let mut guard = engine.buffer_pool().new_page().unwrap();
            guard.page_mut()[0] = 0xAB;
        }

        let wal_dir = tmp.path().join("wal");
        let segments_before = std::fs::read_dir(&wal_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "log"))
            .count();
        assert!(
            segments_before > 1,
            "test precondition failed: expected multiple WAL segments, got {segments_before}"
        );

        engine.trigger_checkpoint().unwrap();

        // After checkpoint, the number of retained segments should be reduced.
        let segments_after = std::fs::read_dir(&wal_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "log"))
            .count();
        assert!(
            segments_after < segments_before,
            "old WAL segments were not recycled: before={segments_before}, after={segments_after}"
        );
    }

    #[test]
    fn recover_repairs_torn_page_after_checkpoint() {
        use std::io::{Seek, SeekFrom, Write};
        use std::mem;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = StorageConfig::new(tmp.path());
        // Use a tiny buffer pool so that the original page is evicted quickly.
        config.buffer_pool_size = 256 * 1024; // 32 frames

        let page_id = {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();

            // 1. Allocate and modify a page.
            let mut guard = engine.buffer_pool().new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut().fill(0xCD);
            drop(guard);

            // 2. First checkpoint: establishes checkpoint_lsn and flushes the page.
            engine.trigger_checkpoint().unwrap();

            // 3. Evict the page so that the next pin_mut sees it as "old" and
            //    writes a FullPageImage (because page_lsn < checkpoint_lsn).
            let frame_count = engine.buffer_pool().frame_count();
            for _ in 0..frame_count + 4 {
                drop(engine.buffer_pool().new_page().unwrap());
            }

            // 4. Reload and modify the page. The first pin_mut writes an FPI.
            {
                let mut guard = engine.buffer_pool().pin_mut(id).unwrap();
                guard.page_mut().fill(0xCD);
            }

            // 5. Ensure the FPI is durable in the WAL. We do not need a second
            //    checkpoint; the recovery replay will apply the FPI directly.
            engine.wal_writer().flush().unwrap();

            // Simulate kill -9: do not run Drop / graceful shutdown. This leaks
            // the WalWriter background thread, but the process exits shortly and
            // the OS reaps it. A more realistic crash is exercised by the
            // fork+kill integration tests in tests/crash_recovery.rs; this unit
            // test keeps the torn-page repair path fast and self-contained.
            mem::forget(engine);
            id
        };

        // Corrupt the first half of the page in the data file (torn write).
        let data_file_path = tmp.path().join("data").join("datafile");
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&data_file_path)
            .unwrap();
        let offset = (page_id.0 - 1) * crate::types::PAGE_SIZE as u64;
        file.seek(SeekFrom::Start(offset)).unwrap();
        let half = vec![0xFFu8; crate::types::PAGE_SIZE / 2];
        file.write_all(&half).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // Recovery replays the FPI, repairing the torn page.
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let guard = engine.buffer_pool().pin(page_id).unwrap();
        assert!(
            guard.page().iter().all(|&b| b == 0xCD),
            "FPI did not repair the torn page"
        );
    }

    #[test]
    fn background_checkpoint_flushes_dirty_pages() {
        use std::time::Duration;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = StorageConfig::new(tmp.path());
        // Short interval so the test does not have to wait long.
        config.checkpoint_interval_ms = 200;
        config.wal_group_commit_timeout_ms = 1;
        config.wal_group_commit_batch_size = 1;

        let page_id = {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            engine.start_background_checkpointing().unwrap();

            let mut guard = engine.buffer_pool().new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[0..8].copy_from_slice(b"bgckpt01");
            drop(guard);

            // Wait for at least one background checkpoint to run.
            std::thread::sleep(Duration::from_millis(600));

            id
        };

        // Reopen without an explicit manual checkpoint: the background thread
        // should have persisted the page.
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let guard = engine.buffer_pool().pin(page_id).unwrap();
        assert_eq!(&guard.page()[0..8], b"bgckpt01");
    }

    #[test]
    fn concurrent_new_page_and_pin_are_safe() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;

        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = StorageConfig::new(tmp.path());
        // Small buffer pool to force eviction pressure.
        config.buffer_pool_size = 256 * 1024; // 32 frames
        config.wal_group_commit_timeout_ms = 1;
        config.wal_group_commit_batch_size = 8;

        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let engine = Arc::new(engine);

        let successes = Arc::new(AtomicUsize::new(0));
        let all_ids: Arc<Mutex<Vec<PageId>>> = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();

        for _ in 0..64 {
            let e = Arc::clone(&engine);
            let s = Arc::clone(&successes);
            let ids = Arc::clone(&all_ids);
            handles.push(thread::spawn(move || {
                for _ in 0..25 {
                    if let Ok(mut g) = e.buffer_pool().new_page() {
                        g.page_mut()[0] = 0xAB;
                        ids.lock().push(g.page_id());
                        s.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let ids = all_ids.lock();
        assert_eq!(ids.len(), 64 * 25);
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), ids.len(), "duplicate page IDs detected");

        // All successfully allocated pages must be durable after checkpoint.
        let checkpoint_lsn = engine.trigger_checkpoint().unwrap();
        assert!(checkpoint_lsn.is_valid());
    }

    #[test]
    fn multiple_checkpoints_keep_data_consistent() {
        let tmp = tempfile::TempDir::new().unwrap();
        let mut config = StorageConfig::new(tmp.path());
        config.wal_segment_size = 4096;
        config.wal_group_commit_timeout_ms = 1;
        config.wal_group_commit_batch_size = 1;

        let ids = {
            let engine = StorageEngine::open(tmp.path(), &config).unwrap();
            let mut ids = Vec::new();
            for i in 0..16u8 {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                guard.page_mut()[0] = i;
                ids.push(guard.page_id());
            }

            // First checkpoint.
            engine.trigger_checkpoint().unwrap();

            // Modify a subset after the first checkpoint.
            for (idx, id) in ids.iter().enumerate() {
                if idx % 2 == 0 {
                    let mut guard = engine.buffer_pool().pin_mut(*id).unwrap();
                    guard.page_mut()[1] = 0xCC;
                }
            }

            // Second checkpoint; WAL recycling is verified by the dedicated
            // `checkpoint_recycles_old_wal_segments` test above.
            engine.trigger_checkpoint().unwrap();
            ids
        };

        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        for (idx, id) in ids.iter().enumerate() {
            let guard = engine.buffer_pool().pin(*id).unwrap();
            assert_eq!(guard.page()[0], idx as u8);
            if idx % 2 == 0 {
                assert_eq!(guard.page()[1], 0xCC);
            }
        }
    }
}
