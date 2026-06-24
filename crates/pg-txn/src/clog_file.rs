//! On-disk CLOG segment files (M2b Stage L, tech-selection §6.2).
//!
//! The commit log lives in `{data_dir}/clog/clog-{segment_id:08}.log` segment
//! files of 128 MiB each. Every XID occupies **4 bits**; each byte holds two
//! XIDs:
//!
//! ```text
//! byte N:
//!   high 4 bits (bits 4..7) → XID = segment_base + 2N + 0   (even XID)
//!   low  4 bits (bits 0..3) → XID = segment_base + 2N + 1   (odd  XID)
//! ```
//!
//! 4-bit state encoding (matches `pg_storage::clog::TxnState`'s
//! `#[repr(u8)]`):
//!
//! ```text
//! 0b0000  IN_PROGRESS
//! 0b0001  COMMITTED
//! 0b0010  ABORTED
//! 0b0011  SUB_COMMITTED (reserved, M3)
//! ```
//!
//! Address math (§6.2):
//!
//! - segment id            = `xid / XIDS_PER_SEGMENT` (= `xid / 268_435_456`)
//! - byte offset in segment = `(xid % XIDS_PER_SEGMENT) / 2`
//! - nibble selector        = `xid & 1` (0 → high nibble, 1 → low nibble)
//!
//! Because a segment is exactly 16,384 CLOG pages, page-granularity math is
//! equally clean: CLOG page = `xid / XIDS_PER_CLOG_PAGE`, and a page never
//! straddles a segment boundary.
//!
//! Segment files are created on first touch and preallocated to the full
//! 128 MiB with `set_len` (sparse on APFS/ext4), so page reads never hit
//! EOF: an untouched region reads back as zeros, which is exactly
//! `IN_PROGRESS` for every XID it covers — no existence check needed.
//!
//! All I/O goes through [`PositionedFile`] (`pread`/`pwrite`), so the handles
//! are cursor-free and safe to share across threads.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;

use pg_storage::clog::TxnState;
use pg_storage::error::{Result, StorageError};
use pg_storage::positioned_file::PositionedFile;
use pg_storage::types::TxnId;

/// CLOG page size in bytes — one [`crate::ClogBuffer`] frame (§6.3).
pub const CLOG_PAGE_BYTES: u64 = 8192;

/// XIDs whose states fit in one CLOG page: 8 KiB × 2 XIDs/byte (§6.3).
pub const XIDS_PER_CLOG_PAGE: u64 = CLOG_PAGE_BYTES * 2; // 16,384

/// XIDs whose states fit in one segment: 128 MiB × 2 XIDs/byte (§6.2).
pub const XIDS_PER_SEGMENT: u64 = 268_435_456;

/// Segment file size in bytes (128 MiB), preallocated sparsely on first touch.
pub const CLOG_SEGMENT_BYTES: u64 = XIDS_PER_SEGMENT / 2; // 134,217,728

/// CLOG pages per segment — a page never straddles a segment boundary.
pub const CLOG_PAGES_PER_SEGMENT: u64 = CLOG_SEGMENT_BYTES / CLOG_PAGE_BYTES; // 16,384

/// Segment id holding `xid`: `xid / 268_435_456` (§6.2).
pub fn segment_id_of_xid(xid: TxnId) -> u64 {
    xid.0 / XIDS_PER_SEGMENT
}

/// Byte offset of `xid`'s nibble within its segment: `(xid % 268_435_456) / 2`.
pub fn byte_offset_of_xid(xid: TxnId) -> u64 {
    (xid.0 % XIDS_PER_SEGMENT) / 2
}

/// Global CLOG page number holding `xid`: `xid / 16_384` (§6.3 frame id).
pub fn page_no_of_xid(xid: TxnId) -> u64 {
    xid.0 / XIDS_PER_CLOG_PAGE
}

/// Byte offset of `xid`'s nibble within its CLOG page.
pub fn byte_in_page_of_xid(xid: TxnId) -> usize {
    ((xid.0 % XIDS_PER_CLOG_PAGE) / 2) as usize
}

/// Segment id holding CLOG page `page_no`.
pub fn segment_id_of_page(page_no: u64) -> u64 {
    page_no / CLOG_PAGES_PER_SEGMENT
}

/// Byte offset of CLOG page `page_no` within its segment.
pub fn page_offset_in_segment(page_no: u64) -> u64 {
    (page_no % CLOG_PAGES_PER_SEGMENT) * CLOG_PAGE_BYTES
}

/// Path of segment `segment_id` under the CLOG directory:
/// `clog-{segment_id:08}.log` (§6.2).
pub fn segment_path(clog_dir: &Path, segment_id: u64) -> PathBuf {
    clog_dir.join(format!("clog-{segment_id:08}.log"))
}

/// Extract the 4-bit state nibble for `xid` from `byte`.
///
/// Bit order (§6.2, v2-clarified): **high nibble = even XID, low nibble =
/// odd XID**. Getting this direction wrong mirrors the entire CLOG.
pub fn get_nibble(byte: u8, xid: TxnId) -> u8 {
    if xid.0 & 1 == 0 {
        byte >> 4
    } else {
        byte & 0x0F
    }
}

/// Return `byte` with the 4-bit state nibble for `xid` replaced by `state`
/// (the `TxnState as u8` value, 0..=3). The other XID's nibble is preserved.
pub fn set_nibble(byte: u8, xid: TxnId, state: u8) -> u8 {
    if xid.0 & 1 == 0 {
        (byte & 0x0F) | ((state & 0x0F) << 4)
    } else {
        (byte & 0xF0) | (state & 0x0F)
    }
}

/// Decode a 4-bit nibble into a [`TxnState`].
///
/// Nibbles are 4 bits so only 0..=15 can occur; values above 3 are not valid
/// states and indicate on-disk corruption, which is unrecoverable here.
pub fn txn_state_from_nibble(nibble: u8) -> TxnState {
    match nibble {
        0 => TxnState::InProgress,
        1 => TxnState::Committed,
        2 => TxnState::Aborted,
        3 => TxnState::SubCommitted,
        other => panic!("corrupt CLOG nibble {other:#06b}: not a valid TxnState"),
    }
}

/// Lazily-opened cache of CLOG segment files under `{data_dir}/clog/`.
///
/// Segments are opened (and sparsely preallocated to 128 MiB) on first touch
/// and kept open for the lifetime of the store. All page I/O is positional
/// (`read_exact_at` / `write_all_at`), so no cursor state is shared.
///
/// # Durability tracking (Stage L review)
///
/// `write_page` deliberately does NOT fsync, but a completed write that is
/// never fsynced is a durability leak: the checkpoint flush used to fsync
/// only "segments of frames dirty *right now*", so both an fsync-failure
/// retry and an eviction writeback (cache clean by flush time) could leave
/// pages written to the page cache but never fsynced — then a checkpoint
/// completes, recycles the WAL, and a crash loses committed state with no
/// way to rebuild it. The store therefore tracks every segment with
/// written-but-unsynced pages in `unsynced_segments`; [`Self::sync_unsynced`]
/// fsyncs ALL of them and removes each only after its fsync succeeded.
#[derive(Debug)]
pub struct ClogSegmentStore {
    clog_dir: PathBuf,
    segments: Mutex<HashMap<u64, Arc<PositionedFile>>>,
    /// Segments holding pages that were written (via `write_page`, including
    /// eviction writeback) but have not been durably fsynced yet.
    unsynced_segments: Mutex<std::collections::HashSet<u64>>,
}

impl ClogSegmentStore {
    /// Open the store rooted at `{data_dir}/clog/`, creating the directory if
    /// needed. Existing segment files are never truncated.
    pub fn open(data_dir: &Path) -> Result<Self> {
        let clog_dir = data_dir.join("clog");
        std::fs::create_dir_all(&clog_dir).map_err(StorageError::Io)?;
        Ok(Self {
            clog_dir,
            segments: Mutex::new(HashMap::new()),
            unsynced_segments: Mutex::new(std::collections::HashSet::new()),
        })
    }

    /// The CLOG directory this store is rooted at.
    pub fn clog_dir(&self) -> &Path {
        &self.clog_dir
    }

    /// Handle to segment `segment_id`, opening and preallocating it on first
    /// touch. Preallocation only ever grows the file, so reopening an
    /// existing full-size segment is a no-op.
    fn segment(&self, segment_id: u64) -> Result<Arc<PositionedFile>> {
        let mut segments = self.segments.lock();
        if let Some(file) = segments.get(&segment_id) {
            return Ok(Arc::clone(file));
        }
        let file = Arc::new(PositionedFile::open(segment_path(
            &self.clog_dir,
            segment_id,
        ))?);
        if file.len()? < CLOG_SEGMENT_BYTES {
            file.set_len(CLOG_SEGMENT_BYTES)?;
        }
        segments.insert(segment_id, Arc::clone(&file));
        Ok(file)
    }

    /// Read the 8 KiB CLOG page `page_no` into `buf`.
    ///
    /// Never returns EOF: the segment is preallocated, so an untouched page
    /// reads as all-zeros — `IN_PROGRESS` for every XID it covers.
    pub fn read_page(&self, page_no: u64, buf: &mut [u8; CLOG_PAGE_BYTES as usize]) -> Result<()> {
        let file = self.segment(segment_id_of_page(page_no))?;
        file.read_exact_at(buf, page_offset_in_segment(page_no))
    }

    /// Write the 8 KiB CLOG page `page_no` back to its segment.
    ///
    /// Does **not** fsync — fsync happens only in
    /// [`crate::ClogBuffer::flush_dirty`], the checkpoint-driven flush
    /// (tech-selection §6.4, v2.3-21). The segment is recorded in
    /// `unsynced_segments` so the flush can find it again even if the frame
    /// that produced the write has since been evicted (see the struct docs).
    pub fn write_page(&self, page_no: u64, buf: &[u8; CLOG_PAGE_BYTES as usize]) -> Result<()> {
        let file = self.segment(segment_id_of_page(page_no))?;
        file.write_all_at(buf, page_offset_in_segment(page_no))?;
        self.unsynced_segments
            .lock()
            .insert(segment_id_of_page(page_no));
        Ok(())
    }

    /// `fsync` segment `segment_id` if it is open. No-op for segments that
    /// were never touched.
    pub fn sync_segment(&self, segment_id: u64) -> Result<()> {
        let segments = self.segments.lock();
        match segments.get(&segment_id) {
            Some(file) => file.sync_all(),
            None => Ok(()),
        }
    }

    /// fsync every segment with written-but-unsynced pages, removing each
    /// from the tracking set only after its fsync succeeds.
    ///
    /// This is the durability backstop the checkpoint flush
    /// ([`crate::ClogBuffer::flush_dirty`]) must end with: it covers both the
    /// segments dirtied by the current flush AND any segment left unsynced
    /// by an eviction writeback or a previous failed attempt. On failure the
    /// remaining segments stay tracked, so the next retry resumes where this
    /// call stopped instead of declaring success over unsynced data.
    pub fn sync_unsynced(&self) -> Result<()> {
        let pending: Vec<u64> = self.unsynced_segments.lock().iter().copied().collect();
        for segment_id in pending {
            self.sync_segment(segment_id)?;
            self.unsynced_segments.lock().remove(&segment_id);
        }
        Ok(())
    }

    /// Number of segments with written-but-unsynced pages (observability and
    /// tests).
    pub fn unsynced_segment_count(&self) -> usize {
        self.unsynced_segments.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xid_address_math_boundaries() {
        // Page 0 covers XIDs 0..=16383.
        assert_eq!(page_no_of_xid(TxnId(0)), 0);
        assert_eq!(page_no_of_xid(TxnId(16383)), 0);
        assert_eq!(page_no_of_xid(TxnId(16384)), 1);
        // Segment 0 covers XIDs 0..=268_435_455.
        assert_eq!(segment_id_of_xid(TxnId(0)), 0);
        assert_eq!(segment_id_of_xid(TxnId(XIDS_PER_SEGMENT - 1)), 0);
        assert_eq!(segment_id_of_xid(TxnId(XIDS_PER_SEGMENT)), 1);
        // Byte offsets within the segment.
        assert_eq!(byte_offset_of_xid(TxnId(0)), 0);
        assert_eq!(byte_offset_of_xid(TxnId(1)), 0);
        assert_eq!(byte_offset_of_xid(TxnId(2)), 1);
        assert_eq!(
            byte_offset_of_xid(TxnId(XIDS_PER_SEGMENT - 1)),
            CLOG_SEGMENT_BYTES - 1
        );
        assert_eq!(byte_offset_of_xid(TxnId(XIDS_PER_SEGMENT)), 0);
        // Byte offsets within a page.
        assert_eq!(byte_in_page_of_xid(TxnId(16383)), 8191);
        assert_eq!(byte_in_page_of_xid(TxnId(16384)), 0);
        // Page → segment mapping: page 16383 is the last page of segment 0.
        assert_eq!(segment_id_of_page(CLOG_PAGES_PER_SEGMENT - 1), 0);
        assert_eq!(segment_id_of_page(CLOG_PAGES_PER_SEGMENT), 1);
        assert_eq!(page_offset_in_segment(CLOG_PAGES_PER_SEGMENT), 0);
        assert_eq!(
            page_offset_in_segment(CLOG_PAGES_PER_SEGMENT - 1),
            CLOG_SEGMENT_BYTES - CLOG_PAGE_BYTES
        );
    }

    #[test]
    fn nibble_bit_order_high_even_low_odd() {
        // Even XID → high nibble; odd XID → low nibble (§6.2).
        let byte = set_nibble(0x00, TxnId(2), TxnState::Committed as u8);
        assert_eq!(byte, 0x10);
        let byte = set_nibble(byte, TxnId(3), TxnState::Aborted as u8);
        assert_eq!(byte, 0x12);
        assert_eq!(get_nibble(byte, TxnId(2)), 1);
        assert_eq!(get_nibble(byte, TxnId(3)), 2);
        // Setting one nibble never disturbs the other.
        let byte = set_nibble(byte, TxnId(2), TxnState::Aborted as u8);
        assert_eq!(byte, 0x22);
        assert_eq!(get_nibble(byte, TxnId(3)), 2);
    }

    #[test]
    fn segment_file_is_preallocated_and_reads_zero() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = ClogSegmentStore::open(tmp.path()).unwrap();

        let mut page = [0xFFu8; CLOG_PAGE_BYTES as usize];
        store.read_page(0, &mut page).unwrap();
        assert!(page.iter().all(|&b| b == 0), "untouched page must be zeros");

        let path = segment_path(store.clog_dir(), 0);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), CLOG_SEGMENT_BYTES);
    }

    #[test]
    fn write_then_read_page_round_trips() {
        let tmp = tempfile::TempDir::new().unwrap();
        let store = ClogSegmentStore::open(tmp.path()).unwrap();

        let mut page = [0u8; CLOG_PAGE_BYTES as usize];
        page[0] = 0x21;
        page[8191] = 0x13;
        store.write_page(0, &page).unwrap();

        let mut back = [0u8; CLOG_PAGE_BYTES as usize];
        store.read_page(0, &mut back).unwrap();
        assert_eq!(back, page);
        store.sync_segment(0).unwrap();
    }
}
