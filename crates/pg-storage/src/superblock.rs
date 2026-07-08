//! Database superblock management.
//!
//! The superblock stores the anchor state required for recovery:
//! checkpoint LSN, next page ID, next transaction ID, etc.
//!
//! It is stored in a dedicated file (`{data_dir}/superblock`) with two 512-byte
//! copies (A and B). Updates are written to the inactive copy first; the copy
//! with the highest valid `checkpoint_lsn` is considered active on recovery.
//! This protects against torn writes during crash.

use std::fs::{self, File};
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

use crate::error::{Result, StorageError};
use crate::io::sync_dir;
use crate::types::{Lsn, PageId, TxnId};

/// Result of reading the superblock file: the decoded superblock plus the
/// offset of the active copy (0 or [`SUPERBLOCK_SIZE`]).
type ReadResult = (Superblock, usize);

/// Magic number for pg_rust superblocks: "PGRS" in little-endian.
pub const SUPERBLOCK_MAGIC: u32 = 0x5047_5253;

/// On-disk superblock format version.
pub const SUPERBLOCK_VERSION: u32 = 1;

/// Size of a single superblock copy in bytes.
pub const SUPERBLOCK_SIZE: usize = 512;

/// Total size of the superblock file (two copies).
pub const SUPERBLOCK_FILE_SIZE: usize = SUPERBLOCK_SIZE * 2;

/// Database superblock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Superblock {
    /// Format version.
    pub version: u32,
    /// Page size in bytes (must match compile-time PAGE_SIZE).
    pub page_size: u32,
    /// LSN of the most recent successful checkpoint (i.e., `CheckpointBegin`).
    pub checkpoint_lsn: Lsn,
    /// Next page ID to allocate.
    pub next_page_id: PageId,
    /// Next transaction ID to allocate.
    pub next_txn_id: TxnId,
    /// Database creation timestamp (Unix epoch nanoseconds).
    pub created_at: u64,
}

impl Superblock {
    /// Create a fresh superblock for a new database.
    pub fn new(page_size: u32) -> Self {
        Self {
            version: SUPERBLOCK_VERSION,
            page_size,
            checkpoint_lsn: Lsn::INVALID,
            next_page_id: PageId::FIRST,
            next_txn_id: TxnId::FIRST,
            created_at: now_nanos(),
        }
    }

    /// Return the path to the superblock file inside `data_dir`.
    pub fn path(data_dir: &Path) -> PathBuf {
        data_dir.join("superblock")
    }

    /// Initialize a new superblock file with two identical copies.
    pub fn create(path: &Path, page_size: u32) -> Result<Self> {
        let parent = path.parent();
        if let Some(parent) = parent {
            fs::create_dir_all(parent).map_err(StorageError::Io)?;
        }

        let sb = Self::new(page_size);
        let mut file = File::create(path).map_err(StorageError::Io)?;
        write_copy(&mut file, 0, &sb)?;
        write_copy(&mut file, SUPERBLOCK_SIZE, &sb)?;
        file.sync_all().map_err(StorageError::Io)?;
        if let Some(parent) = parent {
            sync_dir(parent)?;
        }
        Ok(sb)
    }

    /// Read the superblock file and return the most recent valid copy.
    ///
    /// If both copies are corrupted, returns an error.
    pub fn read(path: &Path) -> Result<Self> {
        read_with_offset(path).map(|(sb, _)| sb)
    }

    /// Update the superblock file on disk.
    ///
    /// The caller should increment `checkpoint_lsn` before calling this
    /// method so that the new copy is selected as active after a crash.
    pub fn write(&self, path: &Path) -> Result<()> {
        // Read the current active copy so we can write to the inactive one.
        let (current, active_offset) = match read_with_offset(path) {
            Ok(result) => result,
            Err(_) => {
                // If we cannot read an existing superblock (e.g., partial
                // initial write), overwrite both copies so that the file is
                // immediately readable again.
                let mut file = fs::OpenOptions::new()
                    .write(true)
                    .open(path)
                    .map_err(StorageError::Io)?;
                write_copy(&mut file, 0, self)?;
                write_copy(&mut file, SUPERBLOCK_SIZE, self)?;
                file.sync_all().map_err(StorageError::Io)?;
                return Ok(());
            }
        };

        // Reject non-increasing checkpoint_lsn. Equal LSN is also rejected
        // because the A/B copy selection relies on strict monotonicity: if two
        // copies had the same checkpoint_lsn, `read_with_offset` would
        // deterministically prefer copy A even when copy B contained newer data.
        if self.checkpoint_lsn.0 <= current.checkpoint_lsn.0 {
            return Err(StorageError::CheckpointFailed(format!(
                "new checkpoint_lsn {} is not greater than current {}",
                self.checkpoint_lsn.0, current.checkpoint_lsn.0
            )));
        }

        let inactive_offset = if active_offset == 0 {
            SUPERBLOCK_SIZE
        } else {
            0
        };

        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(path)
            .map_err(StorageError::Io)?;

        write_copy(&mut file, inactive_offset, self)?;
        file.sync_all().map_err(StorageError::Io)?;
        Ok(())
    }

    /// Return the redo LSN (same as `checkpoint_lsn`).
    pub fn redo_lsn(&self) -> Lsn {
        self.checkpoint_lsn
    }
}

fn write_copy(file: &mut File, offset: usize, sb: &Superblock) -> Result<()> {
    let mut buf = [0u8; SUPERBLOCK_SIZE];
    encode(sb, &mut buf);
    file.seek(std::io::SeekFrom::Start(offset as u64))
        .map_err(StorageError::Io)?;
    file.write_all(&buf).map_err(StorageError::Io)?;
    Ok(())
}

/// Read the superblock file and return the active copy together with its
/// on-disk offset (0 or [`SUPERBLOCK_SIZE`]).
fn read_with_offset(path: &Path) -> Result<ReadResult> {
    let mut file = File::open(path).map_err(StorageError::Io)?;
    let mut buf = [0u8; SUPERBLOCK_FILE_SIZE];
    file.read_exact(&mut buf).map_err(StorageError::Io)?;

    let copy_a = decode(&buf[0..SUPERBLOCK_SIZE]);
    let copy_b = decode(&buf[SUPERBLOCK_SIZE..SUPERBLOCK_FILE_SIZE]);

    match (copy_a, copy_b) {
        (Ok(a), Ok(b)) => {
            // Pick the copy with the higher checkpoint LSN. If equal,
            // prefer A for determinism.
            if b.checkpoint_lsn.0 > a.checkpoint_lsn.0 {
                Ok((b, SUPERBLOCK_SIZE))
            } else {
                Ok((a, 0))
            }
        }
        (Ok(a), Err(_)) => Ok((a, 0)),
        (Err(_), Ok(b)) => Ok((b, SUPERBLOCK_SIZE)),
        (Err(a_err), Err(b_err)) => Err(StorageError::MetadataCorrupted(format!(
            "both copies are corrupted: copy A: {a_err}; copy B: {b_err}"
        ))),
    }
}

fn encode(sb: &Superblock, buf: &mut [u8; SUPERBLOCK_SIZE]) {
    buf[0..4].copy_from_slice(&SUPERBLOCK_MAGIC.to_le_bytes());
    buf[4..8].copy_from_slice(&sb.version.to_le_bytes());
    buf[8..12].copy_from_slice(&sb.page_size.to_le_bytes());
    // 4 bytes padding to keep the 64-bit fields aligned.
    buf[12..16].copy_from_slice(&0u32.to_le_bytes());
    buf[16..24].copy_from_slice(&sb.checkpoint_lsn.0.to_le_bytes());
    buf[24..32].copy_from_slice(&sb.next_page_id.0.to_le_bytes());
    buf[32..40].copy_from_slice(&sb.next_txn_id.0.to_le_bytes());
    buf[40..48].copy_from_slice(&sb.created_at.to_le_bytes());

    // Compute CRC over everything except the checksum field itself.
    let mut hasher = Hasher::new();
    hasher.update(&buf[0..48]);
    hasher.update(&buf[52..SUPERBLOCK_SIZE]);
    let checksum = hasher.finalize();
    buf[48..52].copy_from_slice(&checksum.to_le_bytes());
}

fn decode(bytes: &[u8]) -> Result<Superblock> {
    if bytes.len() != SUPERBLOCK_SIZE {
        return Err(StorageError::MetadataCorrupted(
            "superblock copy has wrong size".to_string(),
        ));
    }

    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if magic != SUPERBLOCK_MAGIC {
        return Err(StorageError::MetadataCorrupted(format!(
            "bad magic: expected {SUPERBLOCK_MAGIC:#x}, got {magic:#x}"
        )));
    }

    let stored_checksum = u32::from_le_bytes(bytes[48..52].try_into().unwrap());
    let mut hasher = Hasher::new();
    hasher.update(&bytes[0..48]);
    hasher.update(&bytes[52..SUPERBLOCK_SIZE]);
    if hasher.finalize() != stored_checksum {
        return Err(StorageError::MetadataCorrupted(
            "checksum mismatch".to_string(),
        ));
    }

    let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
    if version != SUPERBLOCK_VERSION {
        return Err(StorageError::MetadataCorrupted(format!(
            "unsupported version {version}"
        )));
    }

    let page_size = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let checkpoint_lsn = Lsn(u64::from_le_bytes(bytes[16..24].try_into().unwrap()));
    let next_page_id = PageId(u64::from_le_bytes(bytes[24..32].try_into().unwrap()));
    let next_txn_id = TxnId(u64::from_le_bytes(bytes[32..40].try_into().unwrap()));
    let created_at = u64::from_le_bytes(bytes[40..48].try_into().unwrap());

    Ok(Superblock {
        version,
        page_size,
        checkpoint_lsn,
        next_page_id,
        next_txn_id,
        created_at,
    })
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PAGE_SIZE;
    use tempfile::TempDir;

    #[test]
    fn create_and_read_superblock() {
        let tmp = TempDir::new().unwrap();
        let path = Superblock::path(tmp.path());
        let sb = Superblock::create(&path, PAGE_SIZE as u32).unwrap();
        let read = Superblock::read(&path).unwrap();
        assert_eq!(sb, read);
    }

    #[test]
    fn write_updates_checkpoint_lsn() {
        let tmp = TempDir::new().unwrap();
        let path = Superblock::path(tmp.path());
        let mut sb = Superblock::create(&path, PAGE_SIZE as u32).unwrap();

        sb.checkpoint_lsn = Lsn(1024);
        sb.write(&path).unwrap();

        let read = Superblock::read(&path).unwrap();
        assert_eq!(read.checkpoint_lsn, Lsn(1024));
    }

    #[test]
    fn write_refuses_non_monotonic_checkpoint_lsn() {
        let tmp = TempDir::new().unwrap();
        let path = Superblock::path(tmp.path());
        let mut sb = Superblock::create(&path, PAGE_SIZE as u32).unwrap();

        sb.checkpoint_lsn = Lsn(1024);
        sb.write(&path).unwrap();

        let mut sb2 = sb;
        sb2.checkpoint_lsn = Lsn(512);
        assert!(sb2.write(&path).is_err());
    }

    #[test]
    fn read_survives_corrupted_inactive_copy() {
        let tmp = TempDir::new().unwrap();
        let path = Superblock::path(tmp.path());
        let mut sb = Superblock::create(&path, PAGE_SIZE as u32).unwrap();

        sb.checkpoint_lsn = Lsn(2048);
        sb.write(&path).unwrap();

        // Corrupt the inactive (older) copy without truncating the file.
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(std::io::SeekFrom::Start(0)).unwrap();
        file.write_all(&[0xff; SUPERBLOCK_SIZE]).unwrap();
        drop(file);

        let read = Superblock::read(&path).unwrap();
        assert_eq!(read.checkpoint_lsn, Lsn(2048));
    }

    #[test]
    fn read_survives_corrupted_active_copy() {
        let tmp = TempDir::new().unwrap();
        let path = Superblock::path(tmp.path());
        let mut sb = Superblock::create(&path, PAGE_SIZE as u32).unwrap();

        sb.checkpoint_lsn = Lsn(2048);
        sb.write(&path).unwrap();

        // After the update copy B is active (offset SUPERBLOCK_SIZE).
        // Corrupt the active copy and verify we fall back to copy A.
        let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(std::io::SeekFrom::Start(SUPERBLOCK_SIZE as u64))
            .unwrap();
        file.write_all(&[0xff; SUPERBLOCK_SIZE]).unwrap();
        drop(file);

        let read = Superblock::read(&path).unwrap();
        assert_eq!(read.checkpoint_lsn, Lsn::INVALID);
    }

    #[test]
    fn write_fallback_recovers_truncated_superblock_file() {
        let tmp = TempDir::new().unwrap();
        let path = Superblock::path(tmp.path());

        // Simulate a crash that left the superblock file truncated.
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(&[0xff; 100]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let mut sb = Superblock::new(PAGE_SIZE as u32);
        sb.checkpoint_lsn = Lsn(1024);
        sb.write(&path).unwrap();

        let read = Superblock::read(&path).unwrap();
        assert_eq!(read, sb);
    }
}
