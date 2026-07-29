//! Disk-backed SLRU commit log cache (M2b Stage L, tech-selection §6.3).
//!
//! [`ClogBuffer`] is the M2b implementation of
//! [`pg_storage::clog::ClogAccessor`], replacing the M2a
//! [`crate::clog_mem::InMemoryClogAccessor`] with identical call-site
//! semantics: an XID with no recorded state reads as
//! [`TxnState::InProgress`], and `set_state` records the terminal state.
//!
//! # Design (§6.3)
//!
//! The cache is `N` clock-sweep frames; a frame holds one 8 KiB CLOG page
//! (`xid / 16_384` is the frame's page number) backed by the segment files
//! in [`crate::clog_file`]. CLOG pages are deliberately **not** put through
//! the M1 `BufferPool`: they carry no `pd_lsn`/`pd_checksum`, and mixing
//! them into the `PageId`-indexed pool would corrupt its semantics (§6.3).
//!
//! # Durability (§6.4, v2.3-21)
//!
//! `set_state` only marks a frame dirty — it never writes or fsyncs. Dirty
//! frames are written back (without fsync) when the clock sweep evicts
//! them; the **only** fsync of the CLOG is
//! [`flush_dirty`](ClogBuffer::flush_dirty), which the checkpointer invokes
//! between `CheckpointBegin` and `CheckpointEnd` via the
//! [`pg_storage::clog::ClogFlush`] hook. Bits lost in a crash are rebuilt
//! from `TxnCommit`/`TxnAbort` WAL records by the txn redo handlers
//! (idempotent — see [`crate::redo`]).
//!
//! # Concurrency
//!
//! One `parking_lot::RwLock` guards the whole frame array and page table.
//! M2b is predominantly single-threaded; this is correct (if coarse) under
//! concurrency. Sharding the sweep is Phase 7b work.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

use pg_storage::clog::{ClogAccessor, ClogFlush, TxnState};
use pg_storage::error::Result;
use pg_storage::types::TxnId;

use crate::clog_file::{
    byte_in_page_of_xid, get_nibble, page_no_of_xid, set_nibble, txn_state_from_nibble,
    ClogSegmentStore, CLOG_PAGE_BYTES,
};

/// `page_no` of a frame that has never held a page.
const INVALID_PAGE_NO: u64 = u64::MAX;

/// Minimum accepted frame count (tech-selection §6.3: range [4, 1024]).
const MIN_FRAMES: usize = 4;
/// Maximum accepted frame count (tech-selection §6.3: range [4, 1024]).
const MAX_FRAMES: usize = 1024;

/// One clock-sweep frame: a cached 8 KiB CLOG page.
struct ClogFrame {
    /// Global CLOG page number (`xid / 16_384`), or `INVALID_PAGE_NO`.
    page_no: u64,
    /// Raw page bytes; each byte holds two XIDs' 4-bit states (§6.2).
    data: Box<[u8; CLOG_PAGE_BYTES as usize]>,
    /// Modified since last writeback.
    dirty: bool,
    /// Clock-sweep reference bit (second-chance marker).
    referenced: bool,
}

/// Mutable cache state guarded by the buffer-wide lock.
struct Inner {
    frames: Vec<ClogFrame>,
    /// page_no → frame index.
    page_table: HashMap<u64, usize>,
    /// Clock-sweep hand position.
    clock_hand: usize,
}

/// Disk-backed SLRU `ClogAccessor`: `N` clock-sweep frames over the CLOG
/// segment files.
///
/// # Panics
///
/// [`ClogBuffer::open`] panics if `clog_buffer_frames` is outside
/// [4, 1024] (tech-selection §6.3, v2.3-25).
///
/// `get_state` / `set_state` panic on I/O error: the `ClogAccessor` trait
/// (shared with the infallible M1/M2a implementations) cannot return a
/// `Result`, and a CLOG read/write failure is a storage-level corruption
/// from which the transaction layer cannot proceed. The checkpoint flush
/// path ([`ClogBuffer::flush_dirty`]) does return `Result`.
pub struct ClogBuffer {
    store: ClogSegmentStore,
    inner: RwLock<Inner>,
    /// Cache hits (page already resident), for hit-rate observability.
    hits: AtomicU64,
    /// Cache misses (page had to be loaded from the segment file).
    misses: AtomicU64,
}

impl std::fmt::Debug for ClogBuffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClogBuffer")
            .field("frames", &self.inner.read().frames.len())
            .field("hits", &self.hits.load(Ordering::Relaxed))
            .field("misses", &self.misses.load(Ordering::Relaxed))
            .finish()
    }
}

impl ClogBuffer {
    /// Open a `ClogBuffer` rooted at `{data_dir}/clog/` with
    /// `clog_buffer_frames` clock-sweep frames.
    ///
    /// Frame-count rationale (tech-selection §6.3, v2.3-25): the default of
    /// 8 frames is a 128K-XID window, which covers 100 concurrent
    /// transactions with headroom; production TP (≥1K TPS × 60s transaction
    /// lifetimes plus cold lookbacks) should use 64 (1M XIDs); OLAP with
    /// hour-long scans should use 256 (4M XIDs) to avoid hot/cold thrash.
    ///
    /// # Panics
    ///
    /// Panics if `clog_buffer_frames` is outside [4, 1024] — an invalid
    /// configuration must fail loudly at startup, not degrade at runtime.
    pub fn open(data_dir: impl AsRef<Path>, clog_buffer_frames: usize) -> Result<Self> {
        assert!(
            (MIN_FRAMES..=MAX_FRAMES).contains(&clog_buffer_frames),
            "clog_buffer_frames must be in [{MIN_FRAMES}, {MAX_FRAMES}] \
             (tech-selection §6.3: default 8 = 128K XID window covers 100 \
             concurrent txns; production TP 64; OLAP 256), got {clog_buffer_frames}"
        );
        let frames = (0..clog_buffer_frames)
            .map(|_| ClogFrame {
                page_no: INVALID_PAGE_NO,
                data: Box::new([0u8; CLOG_PAGE_BYTES as usize]),
                dirty: false,
                referenced: false,
            })
            .collect();
        Ok(Self {
            store: ClogSegmentStore::open(data_dir.as_ref())?,
            inner: RwLock::new(Inner {
                frames,
                page_table: HashMap::new(),
                clock_hand: 0,
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        })
    }

    /// Number of clock-sweep frames.
    pub fn frame_count(&self) -> usize {
        self.inner.read().frames.len()
    }

    /// Cumulative cache hits since open.
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Cumulative cache misses since open.
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// `hits / (hits + misses)`; `0.0` before any lookup.
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits();
        let total = hits + self.misses();
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }

    /// Write back every dirty frame to its segment file, then fsync every
    /// segment with written-but-unsynced pages, and clear the dirty flags.
    ///
    /// This is the checkpoint hook's entire job (tech-selection §6.4,
    /// v2.3-21): the checkpointer calls it between `CheckpointBegin` and
    /// `CheckpointEnd` via [`ClogFlush`], and it is the **only** place the
    /// CLOG is fsynced.
    ///
    /// The fsync goes through [`ClogSegmentStore::sync_unsynced`], not just
    /// the segments of frames dirty right now: a frame evicted earlier (its
    /// writeback is not fsynced, §6.4) or a segment left over from a failed
    /// previous flush must be covered too, or a completed checkpoint could
    /// recycle the WAL while those commits exist only in the page cache
    /// (Stage L review P1: restart then reads them as `InProgress`).
    pub fn flush_dirty(&self) -> Result<()> {
        let mut inner = self.inner.write();
        for frame in &mut inner.frames {
            if frame.page_no != INVALID_PAGE_NO && frame.dirty {
                self.store.write_page(frame.page_no, &frame.data)?;
                frame.dirty = false;
            }
        }
        self.store.sync_unsynced()
    }

    /// Number of segments with written-but-unsynced pages (observability and
    /// tests; see [`ClogSegmentStore::sync_unsynced`]).
    pub fn unsynced_segment_count(&self) -> usize {
        self.store.unsynced_segment_count()
    }

    /// Frame index holding `page_no`, loading it from the segment file on a
    /// miss (evicting a victim first if the cache is full).
    fn locate_frame(&self, inner: &mut Inner, page_no: u64) -> Result<usize> {
        if let Some(&idx) = inner.page_table.get(&page_no) {
            self.hits.fetch_add(1, Ordering::Relaxed);
            inner.frames[idx].referenced = true;
            return Ok(idx);
        }
        self.misses.fetch_add(1, Ordering::Relaxed);

        let victim = Self::find_victim(inner);
        if inner.frames[victim].page_no != INVALID_PAGE_NO {
            // Evicted dirty frames are written back but NOT fsynced — fsync
            // is reserved for flush_dirty (§6.4, v2.3-21).
            if inner.frames[victim].dirty {
                self.store
                    .write_page(inner.frames[victim].page_no, &inner.frames[victim].data)?;
            }
            inner.page_table.remove(&inner.frames[victim].page_no);
        }

        let mut data = Box::new([0u8; CLOG_PAGE_BYTES as usize]);
        self.store.read_page(page_no, &mut data)?;
        let frame = &mut inner.frames[victim];
        frame.data = data;
        frame.page_no = page_no;
        frame.dirty = false;
        frame.referenced = true;
        inner.page_table.insert(page_no, victim);
        Ok(victim)
    }

    /// Clock-sweep victim selection (§6.3).
    ///
    /// The hand walks the frames, clearing reference bits as it goes.
    /// Never-used frames are taken immediately. On the first full
    /// revolution only **clean** frames with a clear reference bit are
    /// evictable — dirty frames get a second chance. If a whole revolution
    /// finds no victim (everything dirty), dirty frames become evictable
    /// too; the caller writes the victim back before reuse. This always
    /// terminates: after one revolution every reference bit is clear.
    fn find_victim(inner: &mut Inner) -> usize {
        let n = inner.frames.len();
        let mut allow_dirty = false;
        loop {
            for _ in 0..n {
                let idx = inner.clock_hand;
                inner.clock_hand = (inner.clock_hand + 1) % n;
                let frame = &mut inner.frames[idx];
                if frame.page_no == INVALID_PAGE_NO {
                    return idx;
                }
                if frame.referenced {
                    frame.referenced = false;
                    continue;
                }
                if frame.dirty && !allow_dirty {
                    // Second chance for dirty frames on this revolution.
                    continue;
                }
                return idx;
            }
            // Full revolution without a victim: everything is dirty, so
            // allow dirty eviction (with writeback) on the next pass.
            allow_dirty = true;
        }
    }
}

impl ClogAccessor for ClogBuffer {
    fn get_state(&self, xid: TxnId) -> TxnState {
        // XID 0 is InvalidTxnId; its nibble is permanently 0 (§6.2).
        if xid == TxnId::INVALID {
            return TxnState::InProgress;
        }
        let page_no = page_no_of_xid(xid);
        let mut inner = self.inner.write();
        let idx = self
            .locate_frame(&mut inner, page_no)
            .unwrap_or_else(|e| panic!("CLOG read of page {page_no} for {xid:?} failed: {e}"));
        let byte = inner.frames[idx].data[byte_in_page_of_xid(xid)];
        txn_state_from_nibble(get_nibble(byte, xid))
    }

    fn set_state(&self, xid: TxnId, state: TxnState) {
        // XID 0's nibble is permanently 0 (§6.2): writes are ignored.
        if xid == TxnId::INVALID {
            return;
        }
        let page_no = page_no_of_xid(xid);
        let mut inner = self.inner.write();
        let idx = self
            .locate_frame(&mut inner, page_no)
            .unwrap_or_else(|e| panic!("CLOG write of page {page_no} for {xid:?} failed: {e}"));
        let i = byte_in_page_of_xid(xid);
        let frame = &mut inner.frames[idx];
        frame.data[i] = set_nibble(frame.data[i], xid, state as u8);
        frame.dirty = true;
    }
}

impl ClogFlush for ClogBuffer {
    fn flush_dirty(&self) -> Result<()> {
        ClogBuffer::flush_dirty(self)
    }
}

const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    const fn check() {
        assert_send_sync::<ClogBuffer>();
    }
    let _ = check;
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_frame_counts() {
        let tmp = tempfile::TempDir::new().unwrap();
        assert!(ClogBuffer::open(tmp.path(), MIN_FRAMES).is_ok());
        assert!(ClogBuffer::open(tmp.path(), MAX_FRAMES).is_ok());
        assert!(std::panic::catch_unwind(|| ClogBuffer::open(tmp.path(), 3)).is_err());
        assert!(std::panic::catch_unwind(|| ClogBuffer::open(tmp.path(), 1025)).is_err());
    }

    #[test]
    fn invalid_xid_is_always_in_progress_and_ignores_writes() {
        let tmp = tempfile::TempDir::new().unwrap();
        let clog = ClogBuffer::open(tmp.path(), MIN_FRAMES).unwrap();
        clog.set_state(TxnId::INVALID, TxnState::Committed);
        assert_eq!(clog.get_state(TxnId::INVALID), TxnState::InProgress);
    }
}
