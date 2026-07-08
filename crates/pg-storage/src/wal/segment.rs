//! WAL segment file management.
//!
//! A WAL segment is a fixed-size file that stores a contiguous range of the
//! global WAL byte stream. Given an LSN, the segment ID and file offset are
//! computed directly:
//!
//! ```text
//! segment_id = lsn / segment_size
//! file_offset = lsn % segment_size
//! file_name   = wal-{segment_id + 1:08}.log
//! ```

use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::error::{Result, StorageError};
use crate::io::{preallocate_file, sync_dir};
use crate::types::{Lsn, LSN_ALIGNMENT};

/// Format a WAL segment file name from its zero-based segment ID.
///
/// File names are one-based (`wal-00000001.log`) to match PostgreSQL
/// conventions while the internal [`segment_id`](Lsn::segment_id) stays
/// zero-based.
pub fn wal_filename(segment_id: u64) -> String {
    format!("wal-{:08}.log", segment_id + 1)
}

/// Manager for WAL segment files.
///
/// The manager keeps the currently active segment open for writing. It is not
/// thread-safe by itself; the [`WalWriter`](crate::wal::WalWriter) is expected
/// to serialize access.
#[derive(Debug)]
pub struct WalSegmentManager {
    wal_dir: PathBuf,
    segment_size: u64,
    current_segment_id: u64,
    current_file: File,
}

impl WalSegmentManager {
    /// Open or create the WAL segment manager in `wal_dir`.
    ///
    /// If `wal_dir` does not exist it is created. If no segment files exist,
    /// segment 0 is created.
    pub fn open(wal_dir: impl AsRef<Path>, segment_size: u64) -> Result<Self> {
        let wal_dir = wal_dir.as_ref().to_path_buf();
        if !wal_dir.exists() {
            fs::create_dir_all(&wal_dir).map_err(StorageError::Io)?;
            sync_dir(&wal_dir)?;
        }

        if segment_size == 0 {
            return Err(StorageError::InvalidConfig(
                "wal_segment_size must be > 0".to_string(),
            ));
        }
        if segment_size % LSN_ALIGNMENT != 0 {
            return Err(StorageError::InvalidConfig(format!(
                "wal_segment_size {segment_size} must be a multiple of {LSN_ALIGNMENT}"
            )));
        }

        let current_segment_id = Self::discover_latest_segment_id(&wal_dir)?;
        let current_file = Self::open_segment_file(&wal_dir, current_segment_id, segment_size)?;

        Ok(Self {
            wal_dir,
            segment_size,
            current_segment_id,
            current_file,
        })
    }

    /// Return the directory that holds the segment files.
    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    /// Return the configured segment size in bytes.
    pub fn segment_size(&self) -> u64 {
        self.segment_size
    }

    /// Return the zero-based ID of the currently open segment.
    pub fn current_segment_id(&self) -> u64 {
        self.current_segment_id
    }

    /// Return a mutable reference to the current segment file.
    pub fn current_file(&mut self) -> &mut File {
        &mut self.current_file
    }

    /// Ensure the segment file that contains `lsn` is open for writing.
    ///
    /// Rotates to new segment(s) as necessary. Returns an error if `lsn` falls
    /// in a segment before the current one.
    pub fn ensure_for_write(&mut self, lsn: Lsn) -> Result<&mut File> {
        let target_id = lsn.segment_id(self.segment_size);
        if target_id == self.current_segment_id {
            return Ok(&mut self.current_file);
        }
        if target_id < self.current_segment_id {
            return Err(StorageError::WalWriteFailed(format!(
                "cannot write LSN {} to segment {} which is before current segment {}",
                lsn.0, target_id, self.current_segment_id
            )));
        }

        // Sequential rotation; create intermediate segments if the LSN jumped.
        while self.current_segment_id < target_id {
            self.rotate()?;
        }
        Ok(&mut self.current_file)
    }

    /// Close the current segment and open the next one.
    ///
    /// The old file is closed and a new segment file is created and
    /// preallocated to [`segment_size`](Self::segment_size).
    pub fn rotate(&mut self) -> Result<&mut File> {
        let next_id = self.current_segment_id + 1;
        let file = Self::open_segment_file(&self.wal_dir, next_id, self.segment_size)?;
        // The new segment file has been fsynced; fsync the directory so the
        // file creation itself is durable.
        sync_dir(&self.wal_dir)?;
        self.current_segment_id = next_id;
        self.current_file = file;
        Ok(&mut self.current_file)
    }

    /// Delete segment files whose IDs are strictly less than the segment
    /// containing `lsn`.
    ///
    /// The segment that contains `lsn` itself is preserved. Returns the paths
    /// of the removed files.
    pub fn recycle_before(&mut self, lsn: Lsn) -> Result<Vec<PathBuf>> {
        // Defensive bound: never recycle the currently open segment or any
        // segment beyond it, even if the caller passes an LSN that is larger
        // than the current write position.
        let cutoff = lsn
            .segment_id(self.segment_size)
            .min(self.current_segment_id);
        let mut removed = Vec::new();
        for entry in fs::read_dir(&self.wal_dir).map_err(StorageError::Io)? {
            let entry = entry.map_err(StorageError::Io)?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(id) = Self::parse_segment_id(name) else {
                continue;
            };
            if id < cutoff {
                // M1 simply deletes old segments. Phase 7b can switch to a
                // .recycled reuse strategy (rename to wal-NNNNNNNN.log.recycled
                // and repurpose on the next segment creation) to avoid repeated
                // file allocation overhead.
                fs::remove_file(&path).map_err(StorageError::Io)?;
                removed.push(path);
            }
        }
        if !removed.is_empty() {
            sync_dir(&self.wal_dir)?;
        }
        Ok(removed)
    }

    fn discover_latest_segment_id(wal_dir: &Path) -> Result<u64> {
        let mut max_id: Option<u64> = None;
        if wal_dir.exists() {
            for entry in fs::read_dir(wal_dir).map_err(StorageError::Io)? {
                let entry = entry.map_err(StorageError::Io)?;
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if let Some(id) = Self::parse_segment_id(&name) {
                    if max_id.is_none_or(|m| id > m) {
                        max_id = Some(id);
                    }
                }
            }
        }
        Ok(max_id.unwrap_or(0))
    }

    fn parse_segment_id(name: &str) -> Option<u64> {
        let name = name.strip_prefix("wal-")?.strip_suffix(".log")?;
        let one_based: u64 = name.parse().ok()?;
        one_based.checked_sub(1)
    }

    fn open_segment_file(wal_dir: &Path, segment_id: u64, segment_size: u64) -> Result<File> {
        let path = wal_dir.join(wal_filename(segment_id));
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(StorageError::Io)?;

        // Only resize if the file is not already the expected segment size.
        // This avoids redundant ftruncate calls when reopening an existing
        // segment, while still repairing files that are too short or too long.
        let current_len = file.metadata().map_err(StorageError::Io)?.len();
        if current_len != segment_size {
            preallocate_file(&file, segment_size)?;
        }
        file.sync_all().map_err(StorageError::Io)?;
        Ok(file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::TempDir;

    #[test]
    fn wal_filename_is_one_based() {
        assert_eq!(wal_filename(0), "wal-00000001.log");
        assert_eq!(wal_filename(42), "wal-00000043.log");
    }

    #[test]
    fn opens_first_segment_when_empty() {
        let tmp = TempDir::new().unwrap();
        let mgr = WalSegmentManager::open(tmp.path(), 1024).unwrap();
        assert_eq!(mgr.current_segment_id(), 0);
        assert!(tmp.path().join("wal-00000001.log").exists());
    }

    #[test]
    fn rejects_zero_and_unaligned_segment_size() {
        let tmp = TempDir::new().unwrap();
        assert!(WalSegmentManager::open(tmp.path(), 0).is_err());
        assert!(WalSegmentManager::open(tmp.path(), 65).is_err());
    }

    #[test]
    fn ensure_for_write_rotates_when_full() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = WalSegmentManager::open(tmp.path(), 64).unwrap();
        assert_eq!(mgr.current_segment_id(), 0);

        // LSN 56 is at offset 56 in segment 0; a 8-byte record ends exactly at
        // the segment boundary (64).
        let lsn = Lsn(56);
        let file = mgr.ensure_for_write(lsn).unwrap();
        file.seek(SeekFrom::Start(lsn.segment_offset(64))).unwrap();
        file.write_all(b"abcdefgh").unwrap();

        // Next LSN (64) is in segment 1.
        let next_lsn = lsn.advance(8);
        mgr.ensure_for_write(next_lsn).unwrap();
        assert_eq!(mgr.current_segment_id(), 1);

        let file = mgr.current_file();
        file.seek(SeekFrom::Start(next_lsn.segment_offset(64)))
            .unwrap();
        file.write_all(b"12345678").unwrap();

        // Verify both files contain the expected data.
        let data0 = fs::read(tmp.path().join("wal-00000001.log")).unwrap();
        assert_eq!(&data0[56..64], b"abcdefgh");

        let data1 = fs::read(tmp.path().join("wal-00000002.log")).unwrap();
        assert_eq!(&data1[0..8], b"12345678");
    }

    #[test]
    fn recycle_before_deletes_old_segments() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = WalSegmentManager::open(tmp.path(), 64).unwrap();
        mgr.rotate().unwrap();
        mgr.rotate().unwrap();

        assert!(tmp.path().join("wal-00000001.log").exists());
        assert!(tmp.path().join("wal-00000002.log").exists());
        assert!(tmp.path().join("wal-00000003.log").exists());

        // LSN 128 is exactly at the start of segment 2; keep segment 2 and
        // delete segments 0 and 1.
        let removed = mgr.recycle_before(Lsn(128)).unwrap();
        assert_eq!(removed.len(), 2);
        assert!(!tmp.path().join("wal-00000001.log").exists());
        assert!(!tmp.path().join("wal-00000002.log").exists());
        assert!(tmp.path().join("wal-00000003.log").exists());
    }

    #[test]
    fn reopen_discovers_latest_segment() {
        let tmp = TempDir::new().unwrap();
        {
            let mut mgr = WalSegmentManager::open(tmp.path(), 64).unwrap();
            mgr.rotate().unwrap();
            mgr.rotate().unwrap();
        }

        let mgr = WalSegmentManager::open(tmp.path(), 64).unwrap();
        assert_eq!(mgr.current_segment_id(), 2);
    }

    #[test]
    fn recycle_before_never_removes_current_or_future_segments() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = WalSegmentManager::open(tmp.path(), 64).unwrap();
        mgr.rotate().unwrap();
        mgr.rotate().unwrap();

        // All three segments exist; current is segment 2 (wal-00000003.log).
        assert!(tmp.path().join("wal-00000001.log").exists());
        assert!(tmp.path().join("wal-00000002.log").exists());
        assert!(tmp.path().join("wal-00000003.log").exists());

        // Passing an LSN far beyond the current write position must still
        // preserve the current segment (and any future segment). Older segments
        // 0 and 1 are still eligible for recycling.
        let removed = mgr.recycle_before(Lsn(1_000_000)).unwrap();
        assert_eq!(removed.len(), 2);
        assert!(!tmp.path().join("wal-00000001.log").exists());
        assert!(!tmp.path().join("wal-00000002.log").exists());
        assert!(tmp.path().join("wal-00000003.log").exists());
    }

    #[test]
    fn open_repairs_truncated_segment_file() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join(wal_filename(0));
        fs::write(&path, vec![0u8; 100]).unwrap();

        let mgr = WalSegmentManager::open(tmp.path(), 1024).unwrap();
        assert_eq!(mgr.current_segment_id(), 0);
        assert_eq!(fs::metadata(&path).unwrap().len(), 1024);
    }

    #[test]
    fn ensure_for_write_rejects_backward_segment() {
        let tmp = TempDir::new().unwrap();
        let mut mgr = WalSegmentManager::open(tmp.path(), 64).unwrap();
        mgr.rotate().unwrap();
        assert_eq!(mgr.current_segment_id(), 1);

        // LSN 8 falls in segment 0, which is now behind the current segment.
        let err = mgr.ensure_for_write(Lsn(8)).unwrap_err();
        assert!(matches!(err, StorageError::WalWriteFailed(_)));
    }
}
