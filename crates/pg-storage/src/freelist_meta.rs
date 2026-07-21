//! Freelist metadata persistence.
//!
//! `meta/freelist.meta` stores a checkpoint-time snapshot of the page allocator
//! freelist so that recovery can start from a known state and only replay WAL
//! records written after the snapshot.
//!
//! Format (little-endian, hand-written):
///
/// ```text
/// crc32:          u32   (CRC32 over the body below)
/// checkpoint_lsn: u64
/// count:          u64
/// page_ids:       [u64; count]
/// ```
///
/// The CRC32 covers everything after itself (the "body"). A mismatch is a
/// hard failure (`StorageError::MetadataCorrupted`); recovery catches it and
/// rebuilds the freelist from WAL replay.
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::error::{Result, StorageError};
use crate::io::write_atomic;
use crate::types::{Lsn, PageId};

/// Size of the CRC32 prefix in bytes.
const CRC_SIZE: usize = 4;

/// Size of the body header (checkpoint_lsn + count) in bytes.
const BODY_HEADER_SIZE: usize = 16;

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
    ///
    /// Layout: `crc32(4) + body` where `body = checkpoint_lsn(8) + count(8) +
    /// page_ids`.
    pub fn encode(&self) -> Vec<u8> {
        let body_len = BODY_HEADER_SIZE + self.page_ids.len() * 8;
        let mut buf = Vec::with_capacity(CRC_SIZE + body_len);

        // Reserve space for CRC; fill after the body is encoded.
        buf.extend_from_slice(&[0u8; CRC_SIZE]);
        buf.extend_from_slice(&self.checkpoint_lsn.0.to_le_bytes());
        buf.extend_from_slice(&(self.page_ids.len() as u64).to_le_bytes());
        for page_id in &self.page_ids {
            buf.extend_from_slice(&page_id.0.to_le_bytes());
        }

        let crc = crc32fast::hash(&buf[CRC_SIZE..]);
        buf[0..CRC_SIZE].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Decode a snapshot from bytes.
    ///
    /// Verifies the CRC32 over the body. Returns
    /// [`StorageError::MetadataCorrupted`] on CRC mismatch, truncation, or
    /// size mismatch.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < CRC_SIZE + BODY_HEADER_SIZE {
            return Err(StorageError::MetadataCorrupted(TOO_SHORT.to_string()));
        }

        let stored_crc = u32::from_le_bytes(bytes[0..CRC_SIZE].try_into().unwrap());
        let body = &bytes[CRC_SIZE..];
        let computed_crc = crc32fast::hash(body);
        if stored_crc != computed_crc {
            return Err(StorageError::MetadataCorrupted(format!(
                "freelist.meta CRC mismatch: stored {stored_crc:#010x}, computed {computed_crc:#010x}"
            )));
        }

        let checkpoint_lsn = Lsn(u64::from_le_bytes(body[0..8].try_into().unwrap()));
        let count = u64::from_le_bytes(body[8..16].try_into().unwrap()) as usize;

        let expected_body_len = BODY_HEADER_SIZE + count * 8;
        if body.len() != expected_body_len {
            return Err(StorageError::MetadataCorrupted(format!(
                "freelist.meta size mismatch: expected body {expected_body_len}, got {}",
                body.len()
            )));
        }

        let mut page_ids = Vec::with_capacity(count);
        for i in 0..count {
            let offset = BODY_HEADER_SIZE + i * 8;
            page_ids.push(PageId(u64::from_le_bytes(
                body[offset..offset + 8].try_into().unwrap(),
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

    #[test]
    fn decode_rejects_crc_mismatch() {
        let meta = FreelistMeta {
            checkpoint_lsn: Lsn(256),
            page_ids: vec![PageId(1), PageId(7)],
        };
        let mut encoded = meta.encode();
        // Flip a byte in the body (after the 4-byte CRC prefix).
        encoded[10] ^= 0xFF;
        let err = FreelistMeta::decode(&encoded).unwrap_err();
        assert!(
            matches!(err, StorageError::MetadataCorrupted(ref msg) if msg.contains("CRC")),
            "expected CRC mismatch error, got {err:?}"
        );
    }
}
