//! Freelist metadata persistence.
//!
//! `meta/freelist.meta` stores a checkpoint-time snapshot of the page allocator
//! freelist so that recovery can start from a known state and only replay WAL
//! records written after the snapshot.
//!
//! Format (little-endian, hand-written):
///
/// ```text
/// checkpoint_lsn: u64
/// count:          u64
/// page_ids:       [u64; count]
/// ```
///
// TODO(M2): Add CRC32/checksum protection. M1 freelist is always empty
// (there is no free path), and recovery rebuilds it from WAL, so the risk
// of silent corruption causing harm is negligible.
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::error::{Result, StorageError};
use crate::io::write_atomic;
use crate::types::{Lsn, PageId};

/// Decode error message used when freelist.meta is too short.
const TOO_SHORT: &str = "freelist.meta is too short";

/// Freelist snapshot persisted to disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreelistMeta {
    /// LSN up to which this freelist snapshot is valid.
    pub checkpoint_lsn: Lsn,
    /// Page IDs in the freelist at the time of the snapshot.
    pub page_ids: Vec<PageId>,
}

impl FreelistMeta {
    /// Return the default path for the freelist metadata file inside `data_dir`.
    pub fn path(data_dir: &Path) -> PathBuf {
        data_dir.join("meta").join("freelist.meta")
    }

    /// Encode the snapshot to bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16 + self.page_ids.len() * 8);
        buf.extend_from_slice(&self.checkpoint_lsn.0.to_le_bytes());
        buf.extend_from_slice(&(self.page_ids.len() as u64).to_le_bytes());
        for page_id in &self.page_ids {
            buf.extend_from_slice(&page_id.0.to_le_bytes());
        }
        buf
    }

    /// Decode a snapshot from bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 16 {
            return Err(StorageError::MetadataCorrupted(TOO_SHORT.to_string()));
        }

        let checkpoint_lsn = Lsn(u64::from_le_bytes(bytes[0..8].try_into().unwrap()));
        let count = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;

        let expected_len = 16 + count * 8;
        if bytes.len() != expected_len {
            return Err(StorageError::MetadataCorrupted(format!(
                "freelist.meta size mismatch: expected {expected_len}, got {}",
                bytes.len()
            )));
        }

        let mut page_ids = Vec::with_capacity(count);
        for i in 0..count {
            let offset = 16 + i * 8;
            page_ids.push(PageId(u64::from_le_bytes(
                bytes[offset..offset + 8].try_into().unwrap(),
            )));
        }

        Ok(FreelistMeta {
            checkpoint_lsn,
            page_ids,
        })
    }

    /// Write the snapshot atomically to disk.
    pub fn write(&self, path: &Path) -> Result<()> {
        write_atomic(path, &self.encode())
    }

    /// Read the snapshot from disk.
    pub fn read(path: &Path) -> Result<Self> {
        let mut file = File::open(path).map_err(StorageError::Io)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(StorageError::Io)?;
        Self::decode(&bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn roundtrip_empty_freelist() {
        let meta = FreelistMeta {
            checkpoint_lsn: Lsn(128),
            page_ids: vec![],
        };
        let encoded = meta.encode();
        let decoded = FreelistMeta::decode(&encoded).unwrap();
        assert_eq!(meta, decoded);
    }

    #[test]
    fn roundtrip_non_empty_freelist() {
        let meta = FreelistMeta {
            checkpoint_lsn: Lsn(256),
            page_ids: vec![PageId(1), PageId(7), PageId(42)],
        };
        let encoded = meta.encode();
        let decoded = FreelistMeta::decode(&encoded).unwrap();
        assert_eq!(meta, decoded);
    }

    #[test]
    fn write_and_read_file() {
        let tmp = TempDir::new().unwrap();
        let path = FreelistMeta::path(tmp.path());

        let meta = FreelistMeta {
            checkpoint_lsn: Lsn(512),
            page_ids: vec![PageId(10), PageId(20)],
        };
        // `write_atomic` creates the parent `meta/` directory automatically.
        meta.write(&path).unwrap();
        let read = FreelistMeta::read(&path).unwrap();
        assert_eq!(meta, read);
    }

    #[test]
    fn decode_rejects_truncated_data() {
        let bytes = vec![0u8; 8];
        assert!(FreelistMeta::decode(&bytes).is_err());
    }
}
