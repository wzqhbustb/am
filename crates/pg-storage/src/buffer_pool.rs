//! In-memory buffer pool for database pages.
//!
//! The buffer pool caches data pages in a fixed-size array of frames. Pages are
//! located through a sharded page table and evicted using the CLOCK algorithm.
//!
//! # Concurrency model
//!
//! - Lookups on the sharded `page_table` can proceed in parallel for different
//!   shards.
//! - Allocating a frame (for a miss or a new page) requires the global
//!   `allocation_lock`, which serializes eviction and page loading. This keeps
//!   the eviction invariant simple and correct for M1.
//! - Once a page is resident, `pin` / `pin_mut` only touch the frame's own
//!   metadata and content locks.
//! - `pin_mut` waits for exclusive access by acquiring the frame content write
//!   lock. The first mutable access after a page is loaded writes a
//!   `FullPageImage` WAL record before returning the guard.
//!
//! # Lock ordering
//!
//! To avoid deadlocks, locks are acquired in one of two compatible orders:
//!
//! - **Hit path**: `page_table[shard]` → `try_lock(Frame::meta)`. If the frame
//!   metadata is locked by an evictor, the hit falls back to the allocation path
//!   instead of blocking.
//! - **Eviction / allocation path**: `allocation_lock` → `try_lock(Frame::meta)`
//!   → `page_table[shard]` → `data_file` → `Frame::content`.
//!
//! The use of `try_lock` on `Frame::meta` from both directions prevents the
//! classic page-table / frame-meta lock-order reversal deadlock.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::config::StorageConfig;
use crate::error::{Result, StorageError};
use crate::page_allocator::PageAllocator;
use crate::types::{FrameId, Lsn, PageId, PAGE_SIZE};
use crate::wal::record::WalRecord;
use crate::wal::writer::WalWriter;

/// Name of the data file inside `{data_dir}/data/`.
const DATA_FILE_NAME: &str = "datafile";

/// Metadata for a buffer pool frame.
#[derive(Debug)]
struct FrameMeta {
    /// The page currently stored in this frame, or [`PageId::INVALID`] if empty.
    page_id: PageId,
    /// Number of active pins (read or write) on this frame.
    pin_count: u32,
    /// Whether the frame has been modified since it was read from disk.
    dirty: bool,
    /// CLOCK reference bit.
    reference: bool,
    /// LSN of the last WAL record that modified this page.
    page_lsn: Lsn,
    /// True if the page still needs a `FullPageImage` WAL record before the
    /// first modification in this residency.
    needs_fpi: bool,
    /// True if this frame is in the process of being evicted. New pins must
    /// reject the frame even if `page_id` still matches.
    evicting: bool,
}

impl Default for FrameMeta {
    fn default() -> Self {
        Self {
            page_id: PageId::INVALID,
            pin_count: 0,
            dirty: false,
            reference: false,
            page_lsn: Lsn::INVALID,
            needs_fpi: false,
            evicting: false,
        }
    }
}

/// A single slot in the buffer pool.
#[derive(Debug)]
pub struct Frame {
    /// Mutable frame metadata.
    meta: Mutex<FrameMeta>,
    /// Page content. Write access is exclusive; read access may be shared.
    content: RwLock<[u8; PAGE_SIZE]>,
}

impl Default for Frame {
    fn default() -> Self {
        Self {
            meta: Mutex::new(FrameMeta::default()),
            content: RwLock::new([0u8; PAGE_SIZE]),
        }
    }
}

/// In-memory cache of database pages.
#[derive(Debug)]
pub struct BufferPool {
    config: StorageConfig,
    data_file_path: PathBuf,
    data_file: Mutex<File>,
    page_allocator: Arc<Mutex<PageAllocator>>,
    wal_writer: Arc<WalWriter>,
    page_table: Vec<Mutex<HashMap<PageId, FrameId>>>,
    frames: Vec<Frame>,
    clock_hand: AtomicUsize,
    /// Serializes frame allocation / eviction.
    allocation_lock: Mutex<()>,
    /// LSN of the most recent checkpoint begin. Used to decide whether a page
    /// needs a Full Page Image before its first modification in the current
    /// checkpoint cycle.
    checkpoint_lsn: AtomicU64,
}

impl BufferPool {
    /// Open or create the buffer pool.
    ///
    /// `page_allocator` and `wal_writer` are shared with the caller. The buffer
    /// pool opens its own file descriptor to the data file for reads and writes.
    pub fn open(
        data_dir: impl AsRef<Path>,
        config: &StorageConfig,
        page_allocator: Arc<Mutex<PageAllocator>>,
        wal_writer: Arc<WalWriter>,
    ) -> Result<Self> {
        config.validate()?;

        let data_dir = data_dir.as_ref().to_path_buf();
        let data_file_path = data_dir.join("data").join(DATA_FILE_NAME);
        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&data_file_path)
            .map_err(StorageError::Io)?;

        let frame_count = config.buffer_pool_size / config.page_size();
        let frames: Vec<Frame> = (0..frame_count).map(|_| Frame::default()).collect();

        let shards = config.buffer_pool_shards;
        let page_table: Vec<Mutex<HashMap<PageId, FrameId>>> =
            (0..shards).map(|_| Mutex::new(HashMap::new())).collect();

        Ok(Self {
            config: config.clone(),
            data_file_path,
            data_file: Mutex::new(data_file),
            page_allocator,
            wal_writer,
            page_table,
            frames,
            clock_hand: AtomicUsize::new(0),
            allocation_lock: Mutex::new(()),
            checkpoint_lsn: AtomicU64::new(Lsn::INVALID.0),
        })
    }

    /// Return the path to the data file.
    pub fn data_file_path(&self) -> &Path {
        &self.data_file_path
    }

    /// Update the checkpoint LSN used to decide FPI requirements.
    ///
    /// Called by `CheckpointCoordinator` immediately after writing a
    /// `CheckpointBegin` record.
    pub fn set_checkpoint_lsn(&self, lsn: Lsn) {
        self.checkpoint_lsn.store(lsn.0, Ordering::Release);
    }

    /// Return the current checkpoint LSN.
    pub fn checkpoint_lsn(&self) -> Lsn {
        Lsn(self.checkpoint_lsn.load(Ordering::Acquire))
    }

    /// Pin a page for read access.
    ///
    /// If the page is not resident, it is read from disk into a frame. The
    /// returned guard keeps the page pinned until it is dropped.
    pub fn pin(&self, page_id: PageId) -> Result<PageGuard<'_>> {
        if page_id == PageId::INVALID {
            return Err(StorageError::InvalidConfig(
                "cannot pin PageId::INVALID".to_string(),
            ));
        }

        // `locate_or_load` returns a frame that is already pinned and referenced.
        let frame_id = self.locate_or_load(page_id)?;

        let content_guard = self.frames[frame_id.0].content.read();

        Ok(PageGuard {
            frame_id,
            page_id,
            content_guard: Some(content_guard),
            pool: self,
        })
    }

    /// Pin a page for write access.
    ///
    /// The first mutable access after a page is loaded writes a
    /// `FullPageImage` WAL record before the guard is returned. Subsequent
    /// modifications within the same residency do not write additional FPIs.
    pub fn pin_mut(&self, page_id: PageId) -> Result<PageGuardMut<'_>> {
        if page_id == PageId::INVALID {
            return Err(StorageError::InvalidConfig(
                "cannot pin_mut PageId::INVALID".to_string(),
            ));
        }

        // `locate_or_load` returns a frame that is already pinned and referenced.
        let frame_id = self.locate_or_load(page_id)?;

        // Acquire the write lock before writing the FPI so the image reflects
        // the exact state just before this modification.
        let content_guard = self.frames[frame_id.0].content.write();

        // Write FPI if this page has not been modified since the current
        // checkpoint begin. The `needs_fpi` flag tracks whether this residency
        // has already written an FPI; the checkpoint LSN tells us whether the
        // page was last modified in the current checkpoint cycle.
        //
        // If no checkpoint has ever run (`checkpoint_lsn` is invalid), we skip
        // the FPI. This is correct for M1 because pages allocated before the
        // first checkpoint have no prior on-disk version that needs protecting.
        let (needs_fpi, page_lsn) = {
            let meta = self.frames[frame_id.0].meta.lock();
            (meta.needs_fpi, meta.page_lsn)
        };
        let checkpoint_lsn = self.checkpoint_lsn();
        let should_write_fpi = needs_fpi && checkpoint_lsn.is_valid() && page_lsn < checkpoint_lsn;

        if should_write_fpi {
            let image = content_guard.to_vec();
            let fpi_record = match WalRecord::full_page_image(page_id, image) {
                Ok(rec) => rec,
                Err(e) => {
                    // The caller will not receive a guard, so release the pin
                    // that locate_or_load acquired.
                    drop(content_guard);
                    self.unpin(frame_id);
                    return Err(e);
                }
            };
            let fpi_lsn = match self.wal_writer.append(fpi_record) {
                Ok(lsn) => lsn,
                Err(e) => {
                    drop(content_guard);
                    self.unpin(frame_id);
                    return Err(e);
                }
            };

            let mut meta = self.frames[frame_id.0].meta.lock();
            meta.needs_fpi = false;
            meta.page_lsn = fpi_lsn;
            meta.dirty = true;
        }

        Ok(PageGuardMut {
            frame_id,
            page_id,
            content_guard: Some(content_guard),
            pool: self,
        })
    }

    /// Allocate a new page and return it pinned for writing.
    ///
    /// The page content is zero-filled. A new page does not need an FPI
    /// because there is no previous on-disk version to restore.
    pub fn new_page(&self) -> Result<PageGuardMut<'_>> {
        let page_id = {
            let mut allocator = self.page_allocator.lock();
            allocator.alloc_page()?
        };

        let frame_id = self.alloc_frame(page_id, false)?;

        {
            let mut meta = self.frames[frame_id.0].meta.lock();
            // New pages have no on-disk previous version, so they do not need an
            // FPI before the first modification. They are already pinned and
            // referenced by alloc_frame.
            meta.needs_fpi = false;
            meta.dirty = true;
        }

        let content_guard = self.frames[frame_id.0].content.write();
        Ok(PageGuardMut {
            frame_id,
            page_id,
            content_guard: Some(content_guard),
            pool: self,
        })
    }

    /// Flush a dirty page to disk.
    ///
    /// Ensures WAL is fsynced to `frame.page_lsn` before writing the page.
    pub fn flush(&self, page_id: PageId) -> Result<()> {
        if page_id == PageId::INVALID {
            return Err(StorageError::InvalidConfig(
                "cannot flush PageId::INVALID".to_string(),
            ));
        }

        let frame_id = {
            let shard_idx = self.shard_index(page_id);
            let shard = self.page_table[shard_idx].lock();
            *shard
                .get(&page_id)
                .ok_or(StorageError::PageNotFound(page_id))?
        };

        self.flush_frame(frame_id)?;
        Ok(())
    }

    /// Return the number of frames in the pool.
    pub fn frame_count(&self) -> usize {
        self.frames.len()
    }

    /// Return the configured number of page-table shards.
    pub fn shard_count(&self) -> usize {
        self.page_table.len()
    }

    /// Test-only accessor for the `page_lsn` of a resident frame.
    ///
    /// Returns `None` if the page is not currently in the pool. Used by tests
    /// to assert WAL-before-data ordering against the real FPI LSN.
    #[cfg(test)]
    fn frame_page_lsn(&self, page_id: PageId) -> Option<Lsn> {
        let shard_idx = self.shard_index(page_id);
        let frame_id = *self.page_table[shard_idx].lock().get(&page_id)?;
        let meta = self.frames[frame_id.0].meta.lock();
        Some(meta.page_lsn)
    }

    /// Return the page IDs of all currently dirty frames.
    ///
    /// Intended for Stage I's checkpoint coordinator. The returned list is a
    /// snapshot at the time of the call; pages may become clean or dirty again
    /// before the caller observes them. The caller must pin each page before
    /// flushing to observe a consistent state.
    pub fn dirty_page_ids(&self) -> Vec<PageId> {
        self.frames
            .iter()
            .filter_map(|frame| {
                let meta = frame.meta.lock();
                if meta.dirty && meta.page_id != PageId::INVALID {
                    Some(meta.page_id)
                } else {
                    None
                }
            })
            .collect()
    }

    fn shard_index(&self, page_id: PageId) -> usize {
        (page_id.0 as usize) % self.page_table.len()
    }

    /// Locate an existing resident page or load it from disk.
    fn locate_or_load(&self, page_id: PageId) -> Result<FrameId> {
        // Fast path: page is already resident.
        if let Some(frame_id) = self.try_pin_resident(page_id) {
            return Ok(frame_id);
        }

        // Slow path: allocate a frame and read from disk.
        self.alloc_frame(page_id, true)
    }

    /// Try to pin a page that is already resident.
    ///
    /// Returns `Some(frame_id)` if the page was found and pinned. Returns
    /// `None` if the page is not resident, is being evicted, or the frame lock
    /// is contended.
    fn try_pin_resident(&self, page_id: PageId) -> Option<FrameId> {
        let shard_idx = self.shard_index(page_id);
        let shard = self.page_table[shard_idx].lock();
        let frame_id = *shard.get(&page_id)?;

        // Do not block on the frame lock. If an evictor holds it, fall back to
        // the allocation path which serializes with eviction.
        let mut meta = self.frames[frame_id.0].meta.try_lock()?;
        if meta.page_id != page_id || meta.evicting {
            return None;
        }

        meta.pin_count += 1;
        meta.reference = true;
        Some(frame_id)
    }

    /// Allocate a frame for `page_id`.
    ///
    /// Requires the global allocation lock. If `load_from_disk` is true, the
    /// page content is read from the data file; otherwise the frame is left
    /// zero-filled.
    fn alloc_frame(&self, page_id: PageId, load_from_disk: bool) -> Result<FrameId> {
        let _alloc = self.allocation_lock.lock();

        // Double-check: another thread may have loaded the page while we were
        // waiting for the allocation lock.
        if let Some(frame_id) = self.try_pin_resident(page_id) {
            return Ok(frame_id);
        }

        let frame_id = self.evict_frame()?;

        // Reset frame content for a new page.
        {
            let mut content = self.frames[frame_id.0].content.write();
            content.fill(0);
        }

        if load_from_disk {
            self.read_page_from_disk(page_id, frame_id)?;
        }

        {
            let mut meta = self.frames[frame_id.0].meta.lock();
            meta.page_id = page_id;
            // The caller is responsible for filling content; the frame is pinned
            // and referenced before we return.
            meta.pin_count = 1;
            meta.reference = true;
            meta.dirty = false;
            meta.page_lsn = Lsn::INVALID;
            meta.needs_fpi = true;
        }

        {
            let shard_idx = self.shard_index(page_id);
            let mut shard = self.page_table[shard_idx].lock();
            shard.insert(page_id, frame_id);
        }

        Ok(frame_id)
    }

    /// Select a victim frame using CLOCK and evict it.
    fn evict_frame(&self) -> Result<FrameId> {
        let frame_count = self.frames.len();
        if frame_count == 0 {
            return Err(StorageError::BufferPoolFull);
        }

        let max_scans = frame_count * 2;
        for _ in 0..max_scans {
            let hand = self.clock_hand.fetch_add(1, Ordering::Relaxed) % frame_count;

            let mut meta = match self.frames[hand].meta.try_lock() {
                Some(m) => m,
                None => continue,
            };

            if meta.pin_count > 0 || meta.evicting {
                continue;
            }

            if meta.reference {
                meta.reference = false;
                continue;
            }

            // Mark the frame as evicting so new pins reject it even if the
            // page table entry is still visible.
            let old_page_id = meta.page_id;
            let dirty = meta.dirty;
            meta.evicting = true;
            drop(meta);

            // Remove the old mapping from the page table.
            if old_page_id != PageId::INVALID {
                let shard_idx = self.shard_index(old_page_id);
                let mut shard = self.page_table[shard_idx].lock();
                shard.remove(&old_page_id);
            }

            // Flush if dirty. This reads the frame metadata again; `evicting`
            // prevents new pins from succeeding.
            if dirty && old_page_id != PageId::INVALID {
                self.flush_frame(FrameId(hand))?;
            }

            // Reset metadata. The content will be initialized by the caller.
            {
                let mut meta = self.frames[hand].meta.lock();
                *meta = FrameMeta::default();
            }

            return Ok(FrameId(hand));
        }

        Err(StorageError::BufferPoolFull)
    }

    /// Read a page from disk into `frame_id`.
    ///
    /// `page_id` is 1-indexed: page 1 lives at offset 0, page 2 at offset
    /// `PAGE_SIZE`, etc. The data file does not reserve any space for
    /// `PageId(0)`, which is reserved as the invalid sentinel.
    fn read_page_from_disk(&self, page_id: PageId, frame_id: FrameId) -> Result<()> {
        let offset = (page_id.0 - 1) * self.config.page_size() as u64;
        let mut file = self.data_file.lock();
        file.seek(SeekFrom::Start(offset))
            .map_err(StorageError::Io)?;

        let mut content = self.frames[frame_id.0].content.write();
        file.read_exact(&mut *content).map_err(StorageError::Io)?;
        Ok(())
    }

    /// Flush a single frame to disk if it is dirty.
    fn flush_frame(&self, frame_id: FrameId) -> Result<()> {
        let (page_id, page_lsn, dirty) = {
            let meta = self.frames[frame_id.0].meta.lock();
            (meta.page_id, meta.page_lsn, meta.dirty)
        };

        if !dirty || page_id == PageId::INVALID {
            return Ok(());
        }

        // WAL-before-data: ensure the WAL is fsynced up to page_lsn.
        if page_lsn.is_valid() && self.wal_writer.synced_lsn() < page_lsn {
            self.wal_writer.flush_to(page_lsn)?;
        }

        let offset = (page_id.0 - 1) * self.config.page_size() as u64;

        // Lock ordering: data_file before content. The seek only touches the
        // file, so we acquire the file lock first and then the content lock.
        let mut file = self.data_file.lock();
        file.seek(SeekFrom::Start(offset))
            .map_err(StorageError::Io)?;

        let content = self.frames[frame_id.0].content.read();
        file.write_all(&*content).map_err(StorageError::Io)?;
        // TODO(Stage I): checkpoint can batch multiple frame flushes and call
        // sync_all() once at the end. For M1 per-page flush is correct but
        // performs more fsyncs than necessary.
        file.sync_all().map_err(StorageError::Io)?;

        {
            let mut meta = self.frames[frame_id.0].meta.lock();
            meta.dirty = false;
        }

        Ok(())
    }

    fn unpin(&self, frame_id: FrameId) {
        let mut meta = self.frames[frame_id.0].meta.lock();
        debug_assert!(meta.pin_count > 0, "unpin called on unpinned frame");
        meta.pin_count -= 1;
    }
}

/// Read guard for a pinned page.
#[derive(Debug)]
pub struct PageGuard<'a> {
    frame_id: FrameId,
    page_id: PageId,
    content_guard: Option<RwLockReadGuard<'a, [u8; PAGE_SIZE]>>,
    pool: &'a BufferPool,
}

impl PageGuard<'_> {
    /// Return the frame ID held by this guard.
    pub fn frame_id(&self) -> FrameId {
        self.frame_id
    }

    /// Return the page ID held by this guard.
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Return a reference to the page content.
    pub fn page(&self) -> &[u8] {
        &**self.content_guard.as_ref().expect("guard is active")
    }
}

impl AsRef<[u8]> for PageGuard<'_> {
    fn as_ref(&self) -> &[u8] {
        self.page()
    }
}

impl Drop for PageGuard<'_> {
    fn drop(&mut self) {
        // Drop the content lock before decrementing pin_count so the frame
        // cannot be evicted while we still hold the content.
        drop(self.content_guard.take());
        self.pool.unpin(self.frame_id);
    }
}

/// Write guard for a pinned page.
#[derive(Debug)]
pub struct PageGuardMut<'a> {
    frame_id: FrameId,
    page_id: PageId,
    content_guard: Option<RwLockWriteGuard<'a, [u8; PAGE_SIZE]>>,
    pool: &'a BufferPool,
}

impl PageGuardMut<'_> {
    /// Return the frame ID held by this guard.
    pub fn frame_id(&self) -> FrameId {
        self.frame_id
    }

    /// Return the page ID held by this guard.
    pub fn page_id(&self) -> PageId {
        self.page_id
    }

    /// Return a reference to the page content.
    pub fn page(&self) -> &[u8] {
        &**self.content_guard.as_ref().expect("guard is active")
    }

    /// Return a mutable reference to the page content.
    pub fn page_mut(&mut self) -> &mut [u8] {
        &mut **self.content_guard.as_mut().expect("guard is active")
    }
}

impl AsRef<[u8]> for PageGuardMut<'_> {
    fn as_ref(&self) -> &[u8] {
        self.page()
    }
}

impl AsMut<[u8]> for PageGuardMut<'_> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.page_mut()
    }
}

impl Drop for PageGuardMut<'_> {
    fn drop(&mut self) {
        drop(self.content_guard.take());

        // A write guard may have modified the page. We cannot know whether it
        // actually did, so mark the frame dirty on drop. False positives are
        // safe (an unnecessary flush later) and cheaper than tracking every
        // write.
        {
            let mut meta = self.pool.frames[self.frame_id.0].meta.lock();
            if meta.page_id != PageId::INVALID {
                meta.dirty = true;
            }
        }

        self.pool.unpin(self.frame_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
    use proptest::prelude::*;
    use tempfile::TempDir;

    fn test_config(tmp: &TempDir) -> StorageConfig {
        let mut cfg = StorageConfig::new(tmp.path());
        cfg.buffer_pool_size = 1024 * 1024; // 1 MB = 128 frames at 8 KB
        cfg.buffer_pool_shards = 8;
        cfg.wal_group_commit_timeout_ms = 1;
        cfg.wal_group_commit_batch_size = 1;
        cfg
    }

    fn setup(tmp: &TempDir) -> (Arc<Mutex<PageAllocator>>, Arc<WalWriter>, BufferPool) {
        let cfg = test_config(tmp);
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let allocator = Arc::new(Mutex::new(
            PageAllocator::open(tmp.path(), &cfg, Arc::clone(&wal)).unwrap(),
        ));
        let pool =
            BufferPool::open(tmp.path(), &cfg, Arc::clone(&allocator), Arc::clone(&wal)).unwrap();
        (allocator, wal, pool)
    }

    #[test]
    fn open_creates_expected_frame_count() {
        let tmp = TempDir::new().unwrap();
        let cfg = test_config(&tmp);
        let (_, _, pool) = setup(&tmp);
        assert_eq!(pool.frame_count(), cfg.buffer_pool_size / cfg.page_size());
        assert_eq!(pool.shard_count(), cfg.buffer_pool_shards);
    }

    #[test]
    fn new_page_returns_zeroed_writable_page() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let guard = pool.new_page().unwrap();
        assert_eq!(guard.page().len(), PAGE_SIZE);
        assert!(guard.page().iter().all(|&b| b == 0));
    }

    #[test]
    fn pin_read_returns_existing_page() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let mut guard = pool.new_page().unwrap();
        let page_id = guard.page_id();
        guard.page_mut()[0] = 0xAB;
        drop(guard);

        let read_guard = pool.pin(page_id).unwrap();
        assert_eq!(read_guard.page()[0], 0xAB);
    }

    #[test]
    fn pin_mut_writes_full_page_image() {
        let tmp = TempDir::new().unwrap();
        let (_, _wal, pool) = setup(&tmp);

        // Create and populate a page.
        let page_id = {
            let mut guard = pool.new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[0] = 0xCD;
            id
        };

        // Simulate a checkpoint so that the page is considered "before the
        // checkpoint" when it is reloaded. This triggers the FPI path.
        pool.set_checkpoint_lsn(Lsn(1_000));

        // First pin_mut after the page has been evicted should write an FPI.
        // Force eviction by allocating many new pages.
        for _ in 0..pool.frame_count() + 10 {
            let _ = pool.new_page().unwrap();
        }

        let mut guard = pool.pin_mut(page_id).unwrap();
        guard.page_mut()[1] = 0xEF;
        drop(guard);

        // WAL should contain at least one FullPageImage record.
        let mut reader = crate::wal::reader::WalReader::open(
            tmp.path().join("wal"),
            test_config(&tmp).wal_segment_size,
        )
        .unwrap();
        let mut found_fpi = false;
        while let Some(record) = reader.next_record().unwrap() {
            if record.record_type == crate::wal::record::WalRecordType::FullPageImage {
                found_fpi = true;
            }
        }
        assert!(
            found_fpi,
            "pin_mut should have written a FullPageImage record"
        );
    }

    #[test]
    fn flush_persists_page() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let page_id = {
            let mut guard = pool.new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[0..4].copy_from_slice(&[1, 2, 3, 4]);
            id
        };

        pool.flush(page_id).unwrap();

        // Drop the pool and reopen it; the page should still be readable.
        let (_, _, pool2) = setup(&tmp);
        let guard = pool2.pin(page_id).unwrap();
        assert_eq!(&guard.page()[0..4], &[1, 2, 3, 4]);
    }

    #[test]
    fn wal_before_data_on_evict() {
        // Single-frame pool: every page load evicts the resident page, so
        // eviction is fully deterministic. The group-commit worker is
        // configured to flush ONLY on its 1s timeout (the batch size is never
        // reached), so within this test's millisecond-scale critical section
        // the worker cannot fsync the FPI spontaneously: the only way
        // synced_lsn can advance past the FPI LSN is flush_frame's
        // flush_to(page_lsn). That makes the assertion in step 5 a genuine
        // WAL-before-data guard — delete the flush_to in flush_frame and it
        // fails. Each explicit flush_to stalls at most ~1s waiting for the
        // worker's timeout, which bounds the runtime.
        let tmp = TempDir::new().unwrap();
        let mut cfg = StorageConfig::new(tmp.path());
        cfg.buffer_pool_size = cfg.page_size(); // 1 frame
        cfg.buffer_pool_shards = 1;
        cfg.wal_group_commit_timeout_ms = 1_000;
        cfg.wal_group_commit_batch_size = 1_000_000;
        let wal = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());
        let allocator = Arc::new(Mutex::new(
            PageAllocator::open(tmp.path(), &cfg, Arc::clone(&wal)).unwrap(),
        ));
        let pool =
            BufferPool::open(tmp.path(), &cfg, Arc::clone(&allocator), Arc::clone(&wal)).unwrap();

        // 1. Create the victim with on-disk content, then a second page whose
        //    allocation evicts the victim from the single frame (the dirty
        //    eviction writes the victim to the data file).
        let victim_id = {
            let mut guard = pool.new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[0] = 0xAA;
            id
        };
        let other_id = {
            let mut guard = pool.new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[0] = 0x11;
            id
        };
        assert!(
            pool.frame_page_lsn(victim_id).is_none(),
            "victim should have been evicted by the second allocation"
        );

        // 2. Set checkpoint_lsn above everything written so far so that the
        //    next pin_mut on the victim appends an FPI.
        pool.set_checkpoint_lsn(wal.synced_lsn());

        // 3. pin_mut reloads the victim (evicting `other`, whose flush brings
        //    synced_lsn up to date) and appends its FPI without flushing.
        //    Capture the real FPI LSN from the frame metadata. The worker's
        //    next spontaneous flush is ~1s away, so the FPI cannot become
        //    durable on its own within this critical section.
        let fpi_lsn = {
            let mut guard = pool.pin_mut(victim_id).unwrap();
            guard.page_mut()[0] = 0xBB;
            drop(guard);
            pool.frame_page_lsn(victim_id)
                .expect("victim frame must be resident after pin_mut")
        };
        assert!(
            wal.synced_lsn() < fpi_lsn,
            "setup: the FPI must not be durable yet: synced={}, fpi_lsn={}",
            wal.synced_lsn(),
            fpi_lsn
        );

        // 4. Reload `other`, evicting the dirty victim. flush_frame must call
        //    flush_to(fpi_lsn) before writing the page.
        {
            let _guard = pool.pin(other_id).unwrap();
        }
        assert!(
            pool.frame_page_lsn(victim_id).is_none(),
            "victim should have been evicted"
        );

        // 5. WAL-before-data: synced_lsn now covers the real FPI LSN.
        let post_evict_synced = wal.synced_lsn();
        assert!(
            post_evict_synced >= fpi_lsn,
            "eviction must have called flush_to(fpi_lsn) for WAL-before-data: \
             fpi_lsn={fpi_lsn}, post_evict_synced={post_evict_synced}"
        );

        // 6. The victim page is durable on disk with the updated content.
        let guard = pool.pin(victim_id).unwrap();
        assert_eq!(guard.page()[0], 0xBB);
    }

    #[test]
    fn clock_evicts_unreferenced_pages() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let frame_count = pool.frame_count();
        // Allocate exactly frame_count pages and immediately drop the guards.
        // None are referenced, so CLOCK should be able to evict them.
        let mut ids = Vec::new();
        for _ in 0..frame_count {
            let guard = pool.new_page().unwrap();
            ids.push(guard.page_id());
        }
        drop(ids);

        // Allocate one more page; this must succeed by evicting an old frame.
        let guard = pool.new_page().unwrap();
        assert_eq!(guard.page().len(), PAGE_SIZE);
    }

    #[test]
    fn clock_gives_second_chance_to_referenced_pages() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let frame_count = pool.frame_count();
        // Allocate and uniquely mark every frame.
        let mut ids = Vec::new();
        for i in 0..frame_count {
            let mut guard = pool.new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[0] = i as u8;
            ids.push(id);
        }
        drop(ids);

        // Reference a strict subset of the pages.
        let referenced: Vec<_> = (0..frame_count / 2)
            .map(|i| {
                let guard = pool.pin(PageId(i as u64 + 1)).unwrap();
                assert_eq!(guard.page()[0], i as u8);
                guard.page_id()
            })
            .collect();
        drop(referenced);

        // Evict exactly the number of unreferenced pages.
        let unreferenced_count = frame_count - frame_count / 2;
        for _ in 0..unreferenced_count {
            drop(pool.new_page().unwrap());
        }

        // The referenced pages must still be resident (cache hits) with their
        // original content.
        for i in 0..frame_count / 2 {
            let guard = pool.pin(PageId(i as u64 + 1)).unwrap();
            assert_eq!(guard.page()[0], i as u8);
        }
    }

    #[test]
    fn full_scan_does_not_pin_all_pages() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let frame_count = pool.frame_count();
        // Fill the pool with unique data.
        let mut ids = Vec::new();
        for i in 0..frame_count {
            let mut guard = pool.new_page().unwrap();
            let id = guard.page_id();
            guard.page_mut()[0] = i as u8;
            ids.push(id);
        }
        drop(ids);

        // Simulate a full table scan: pin every page once, then release.
        for i in 0..frame_count {
            let guard = pool.pin(PageId(i as u64 + 1)).unwrap();
            assert_eq!(guard.page()[0], i as u8);
        }

        // After one full scan, all pages have reference=true. CLOCK should give
        // each page exactly one second chance, so allocating frame_count more
        // pages should evict all original pages without BufferPoolFull.
        for _ in 0..frame_count {
            drop(pool.new_page().unwrap());
        }
    }

    #[test]
    fn pin_nonexistent_page_returns_error() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        // PageId(1000) was never allocated; the data file has no space for it.
        let result = pool.pin(PageId(1000));
        assert!(result.is_err(), "pinning a non-existent page must fail");
    }

    #[test]
    fn pinned_pages_are_not_evicted() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let frame_count = pool.frame_count();
        // Pin the first page and keep it alive.
        let pinned = pool.new_page().unwrap();

        // Fill the pool with additional pages and immediately release them so
        // they become evictable.
        for _ in 0..frame_count - 1 {
            drop(pool.new_page().unwrap());
        }

        // One more allocation should succeed (evicts one of the previously
        // allocated pages while keeping the pinned page resident).
        let _extra = pool.new_page().unwrap();

        // The originally pinned page must still be accessible.
        assert_eq!(pinned.page().len(), PAGE_SIZE);
    }

    #[test]
    fn concurrent_pin_and_new_page_are_safe() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;

        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);
        let pool = Arc::new(pool);

        let successes = Arc::new(AtomicUsize::new(0));
        let all_ids: Arc<Mutex<Vec<PageId>>> = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();

        for _ in 0..16 {
            let p = Arc::clone(&pool);
            let s = Arc::clone(&successes);
            let ids = Arc::clone(&all_ids);
            handles.push(thread::spawn(move || {
                for _ in 0..50 {
                    if let Ok(g) = p.new_page() {
                        ids.lock().push(g.page_id());
                        s.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(successes.load(Ordering::Relaxed), 16 * 50);

        let ids = all_ids.lock();
        assert_eq!(ids.len(), 16 * 50);
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(
            sorted.len(),
            ids.len(),
            "concurrent new_page returned duplicate page IDs"
        );
    }

    #[test]
    fn dirty_page_ids_reflects_modified_pages() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let mut dirty_ids = Vec::new();
        for _ in 0..3 {
            let guard = pool.new_page().unwrap();
            dirty_ids.push(guard.page_id());
        }

        // New pages are dirty.
        let mut reported = pool.dirty_page_ids();
        reported.sort();
        assert_eq!(reported, dirty_ids);

        // After flush, no pages are dirty.
        for id in &dirty_ids {
            pool.flush(*id).unwrap();
        }
        assert!(pool.dirty_page_ids().is_empty());
    }

    #[test]
    fn repeated_pin_unpin_does_not_leak_pin_count() {
        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);

        let page_id = {
            let guard = pool.new_page().unwrap();
            guard.page_id()
        };

        // Pin and drop the same page many times. If pin_count were leaked,
        // the frame would eventually become unevictable.
        for _ in 0..16 {
            let guard = pool.pin(page_id).unwrap();
            assert_eq!(guard.page().len(), PAGE_SIZE);
            drop(guard);
        }

        // Force eviction by filling the pool and then some. This must not
        // panic with BufferPoolFull.
        let frame_count = pool.frame_count();
        for _ in 0..frame_count + 16 {
            drop(pool.new_page().unwrap());
        }
    }

    #[test]
    fn concurrent_pin_unpin_stress() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;

        let tmp = TempDir::new().unwrap();
        let (_, _, pool) = setup(&tmp);
        let pool = Arc::new(pool);

        // Pre-allocate a set of shared pages for concurrent pin/unpin/new_page
        // traffic, plus one exclusive page per thread so we can verify that
        // written data is readable after the stress burst.
        let mut shared_ids = Vec::new();
        for _ in 0..8 {
            let guard = pool.new_page().unwrap();
            shared_ids.push(guard.page_id());
        }
        let mut owned_ids = Vec::new();
        for _ in 0..100 {
            let mut guard = pool.new_page().unwrap();
            guard.page_mut()[0] = 0; // initial baseline
            owned_ids.push(guard.page_id());
        }

        let ops = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for thread_id in 0..100usize {
            let p = Arc::clone(&pool);
            let shared = shared_ids.clone();
            let owned = owned_ids.clone();
            let o = Arc::clone(&ops);
            handles.push(thread::spawn(move || {
                for i in 0..20usize {
                    let action = (thread_id + i) % 5;
                    match action {
                        0 => {
                            if let Ok(g) = p.pin(shared[i % shared.len()]) {
                                assert_eq!(g.page().len(), PAGE_SIZE);
                                o.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        1 => {
                            if let Ok(mut g) = p.pin_mut(shared[i % shared.len()]) {
                                g.page_mut()[0] = thread_id as u8;
                                o.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        2 => {
                            if let Ok(g) = p.new_page() {
                                o.fetch_add(1, Ordering::Relaxed);
                                drop(g);
                            }
                        }
                        3 => {
                            if let Ok(g) = p.pin(shared[i % shared.len()]) {
                                o.fetch_add(1, Ordering::Relaxed);
                                drop(g);
                            }
                        }
                        _ => {
                            // Write to the thread's exclusive page. The final
                            // value should be the last successful write.
                            if let Ok(mut g) = p.pin_mut(owned[thread_id]) {
                                g.page_mut()[0] = (thread_id + 1) as u8;
                                g.page_mut()[1..9].copy_from_slice(&i.to_be_bytes());
                                o.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // The exact count is not important; the test passes if there are no
        // deadlocks, panics, or data races detected by Miri/TSan/loom in later
        // stages.
        assert!(ops.load(Ordering::Relaxed) > 0);

        // Verify that every thread's exclusive page can be read back and
        // contains one of the values the owning thread wrote.
        for (thread_id, &owned_id) in owned_ids.iter().enumerate() {
            let guard = pool.pin(owned_id).unwrap();
            assert_eq!(guard.page()[0], (thread_id + 1) as u8);
            let last_iteration = u64::from_be_bytes(guard.page()[1..9].try_into().unwrap());
            assert!(last_iteration < 20, "owned page {thread_id} corrupted");
        }

        // Buffer pool must still be usable after the stress burst.
        let guard = pool.new_page().unwrap();
        assert_eq!(guard.page().len(), PAGE_SIZE);

        // And frames must still be evictable (no pin_count leak).
        let frame_count = pool.frame_count();
        for _ in 0..frame_count + 16 {
            drop(pool.new_page().unwrap());
        }
    }

    proptest! {
        // Coding plan target is 10,000 cases. 64 keeps normal CI fast while
        // exercising allocate-write-read invariants; set PROPTEST_CASES to
        // override.
        #![proptest_config(ProptestConfig::with_cases(
            std::env::var("PROPTEST_CASES")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(64)
        ))]

        #[test]
        fn allocated_pages_are_unique_and_readable(count in 1usize..50) {
            let tmp = TempDir::new().unwrap();
            let (_, _, pool) = setup(&tmp);

            let mut ids = Vec::with_capacity(count);
            for i in 0..count {
                let mut guard = pool.new_page().unwrap();
                guard.page_mut()[0] = (i % 256) as u8;
                guard.page_mut()[1..9].copy_from_slice(&(i as u64).to_be_bytes());
                ids.push(guard.page_id());
            }

            prop_assert_eq!(ids.len(), count);
            let mut sorted = ids.clone();
            sorted.sort_unstable();
            sorted.dedup();
            prop_assert_eq!(sorted.len(), ids.len(), "duplicate page IDs");

            for (i, id) in ids.iter().enumerate() {
                let guard = pool.pin(*id).unwrap();
                prop_assert_eq!(guard.page()[0], (i % 256) as u8);
                prop_assert_eq!(&guard.page()[1..9], &(i as u64).to_be_bytes());
            }
        }
    }
}
