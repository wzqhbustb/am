//! Positional I/O wrapper around `std::fs::File`.
//!
//! Replaces the `Mutex<File>` idiom used through M1: every read/write takes an
//! explicit offset, so the file cursor is not shared state and multiple
//! threads can issue concurrent pread/pwrite calls against the same handle.
//!
//! Cross-platform: `read_at` / `write_at` on Unix, `seek_read` / `seek_write`
//! on Windows. On both platforms the methods accept `&self` — Unix pread and
//! pwrite are cursor-neutral by definition; Windows' `seek_read` /
//! `seek_write` mutate the cursor as a side effect, but we never mix
//! positional and sequential I/O on the same handle so the mutation is
//! invisible.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::error::{Result, StorageError};

/// Shared, cursor-free handle to a file used with positional I/O.
///
/// All methods are `&self`; the wrapper is `Send + Sync` so it can be owned
/// directly by a `Sync` container (e.g. `BufferPool` embeds one outright) or
/// shared via `Arc` when independent subsystem roots need the same handle
/// (e.g. `Arc<PositionedFile>` during WAL replay).
#[derive(Debug)]
pub struct PositionedFile {
    file: File,
    path: PathBuf,
}

impl PositionedFile {
    /// Open (or create) `path` with read+write access and no truncation.
    ///
    /// The parent directory must already exist; use `io::ensure_data_dir`
    /// beforehand.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(StorageError::Io)?;
        Ok(Self { file, path })
    }

    /// The path this file was opened from.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Current file length in bytes. Costs one `fstat`; callers that need it
    /// frequently should cache the value themselves.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> Result<u64> {
        self.file
            .metadata()
            .map(|m| m.len())
            .map_err(StorageError::Io)
    }

    /// Read exactly `buf.len()` bytes starting at `offset`.
    ///
    /// Returns `UnexpectedEof` if the file ends mid-read.
    #[cfg(unix)]
    pub fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<()> {
        use std::os::unix::fs::FileExt;
        self.file
            .read_exact_at(buf, offset)
            .map_err(StorageError::Io)
    }

    /// Windows fallback: `FileExt::seek_read` returns partial reads on
    /// interrupt, so we loop.
    #[cfg(windows)]
    pub fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> Result<()> {
        use std::io::{Error, ErrorKind};
        use std::os::windows::fs::FileExt;
        while !buf.is_empty() {
            match self.file.seek_read(buf, offset) {
                Ok(0) => {
                    return Err(StorageError::Io(Error::new(
                        ErrorKind::UnexpectedEof,
                        "failed to fill whole buffer",
                    )));
                }
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(StorageError::Io(e)),
            }
        }
        Ok(())
    }

    /// Write all of `buf` starting at `offset`.
    ///
    /// On POSIX, `pwrite` past the current end-of-file atomically extends the
    /// file. In this codebase we never rely on that: `PageAllocator::set_len`
    /// always pre-extends before a page is written. The note is here for
    /// future readers — if you change allocation patterns, be aware that a
    /// concurrent reader using `read_exact_at` may see zeros in the gap.
    #[cfg(unix)]
    pub fn write_all_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        use std::os::unix::fs::FileExt;
        self.file
            .write_all_at(buf, offset)
            .map_err(StorageError::Io)
    }

    /// Windows fallback: `FileExt::seek_write` returns partial writes on
    /// interrupt, so we loop.
    #[cfg(windows)]
    pub fn write_all_at(&self, mut buf: &[u8], mut offset: u64) -> Result<()> {
        use std::io::{Error, ErrorKind};
        use std::os::windows::fs::FileExt;
        while !buf.is_empty() {
            match self.file.seek_write(buf, offset) {
                Ok(0) => {
                    return Err(StorageError::Io(Error::new(
                        ErrorKind::WriteZero,
                        "failed to write whole buffer",
                    )));
                }
                Ok(n) => {
                    buf = &buf[n..];
                    offset += n as u64;
                }
                Err(e) if e.kind() == ErrorKind::Interrupted => {}
                Err(e) => return Err(StorageError::Io(e)),
            }
        }
        Ok(())
    }

    /// Grow or shrink the file to exactly `new_size` bytes (`ftruncate`).
    ///
    /// In this codebase `set_len` is only called by `PageAllocator` in a
    /// grow-only pattern (always `new_size >= current_size`). Concurrent
    /// `pread`/`pwrite` at offsets below the old file size are safe during a
    /// grow — POSIX guarantees that `ftruncate` extending a file does not
    /// disturb existing data. Callers **must not** shrink while concurrent
    /// readers/writers may reference the truncated region.
    pub fn set_len(&self, new_size: u64) -> Result<()> {
        self.file.set_len(new_size).map_err(StorageError::Io)
    }

    /// `fsync(fd)` — flush data + metadata to disk.
    pub fn sync_all(&self) -> Result<()> {
        self.file.sync_all().map_err(StorageError::Io)
    }
}

const _: () = {
    fn assert_send_sync<T: Send + Sync>() {}
    fn check() {
        assert_send_sync::<PositionedFile>();
    }
    let _ = check;
};

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn path(tmp: &TempDir) -> PathBuf {
        tmp.path().join("pf-test.bin")
    }

    #[test]
    fn open_creates_empty_file() {
        let tmp = TempDir::new().unwrap();
        let pf = PositionedFile::open(path(&tmp)).unwrap();
        assert_eq!(pf.len().unwrap(), 0);
    }

    #[test]
    fn write_then_read_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let pf = PositionedFile::open(path(&tmp)).unwrap();
        pf.set_len(4096).unwrap();

        let data = [0x55u8; 512];
        pf.write_all_at(&data, 1024).unwrap();

        let mut out = [0u8; 512];
        pf.read_exact_at(&mut out, 1024).unwrap();
        assert_eq!(out, data);
    }

    #[test]
    fn read_at_beyond_eof_errors() {
        let tmp = TempDir::new().unwrap();
        let pf = PositionedFile::open(path(&tmp)).unwrap();
        pf.set_len(64).unwrap();

        let mut out = [0u8; 128];
        let err = pf.read_exact_at(&mut out, 0).unwrap_err();
        match err {
            StorageError::Io(e) => {
                assert_eq!(e.kind(), std::io::ErrorKind::UnexpectedEof);
            }
            other => panic!("expected Io UnexpectedEof, got {other:?}"),
        }
    }

    #[test]
    fn set_len_extends_file() {
        let tmp = TempDir::new().unwrap();
        let pf = PositionedFile::open(path(&tmp)).unwrap();
        pf.set_len(8192).unwrap();
        assert_eq!(pf.len().unwrap(), 8192);
        // Extended region reads back as zeros.
        let mut buf = [0xFFu8; 32];
        pf.read_exact_at(&mut buf, 4096).unwrap();
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn sync_all_succeeds() {
        let tmp = TempDir::new().unwrap();
        let pf = PositionedFile::open(path(&tmp)).unwrap();
        pf.set_len(64).unwrap();
        pf.write_all_at(&[1u8; 32], 0).unwrap();
        pf.sync_all().unwrap();
    }

    #[test]
    fn concurrent_writes_at_disjoint_offsets_do_not_race() {
        use std::sync::Arc;
        use std::thread;

        let tmp = TempDir::new().unwrap();
        let pf = Arc::new(PositionedFile::open(path(&tmp)).unwrap());
        pf.set_len(4096 * 16).unwrap();

        let handles: Vec<_> = (0..16)
            .map(|i| {
                let pf = Arc::clone(&pf);
                thread::spawn(move || {
                    let byte = 0x10 + i as u8;
                    let data = [byte; 4096];
                    pf.write_all_at(&data, (i as u64) * 4096).unwrap();
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }

        for i in 0..16 {
            let mut out = [0u8; 4096];
            pf.read_exact_at(&mut out, (i as u64) * 4096).unwrap();
            let expected = 0x10 + i as u8;
            assert!(
                out.iter().all(|&b| b == expected),
                "block {i} has mixed bytes"
            );
        }
    }

    #[test]
    fn path_accessor_returns_open_path() {
        let tmp = TempDir::new().unwrap();
        let p = path(&tmp);
        let pf = PositionedFile::open(&p).unwrap();
        assert_eq!(pf.path(), p.as_path());
    }
}
