//! Layer 1 storage error types.

use std::io;

use thiserror::Error;

use crate::types::{Lsn, PageId};

/// Errors returned by the storage layer.
#[derive(Debug, Error)]
pub enum StorageError {
    /// An underlying I/O operation failed.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// The requested page does not exist in the data file.
    #[error("page {0} not found")]
    PageNotFound(PageId),

    /// The buffer pool is full and no frame can be evicted.
    #[error("buffer pool full, no evictable frame")]
    BufferPoolFull,

    /// The WAL is corrupted at the given LSN (CRC mismatch, truncation, etc.).
    #[error("WAL corrupted at {0}")]
    WalCorrupted(Lsn),

    /// A WAL write operation failed.
    #[error("WAL write failed: {0}")]
    WalWriteFailed(String),

    /// A WAL read operation failed.
    #[error("WAL read failed: {0}")]
    WalReadFailed(String),

    /// A checkpoint operation failed.
    #[error("checkpoint failed: {0}")]
    CheckpointFailed(String),

    /// Metadata file (superblock, freelist.meta, etc.) is corrupted or unreadable.
    #[error("metadata corrupted: {0}")]
    MetadataCorrupted(String),

    /// A requested LSN has not been written yet.
    #[error("LSN {0} not available")]
    LsnNotAvailable(Lsn),

    /// The configuration is invalid.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// A serialization or deserialization error (e.g. bincode payload).
    #[error("serialization error: {0}")]
    Serialize(String),

    /// Recovery must be completed before the requested operation.
    #[error("recovery required: {0}")]
    RecoveryRequired(String),
}

/// A convenient type alias for storage-layer results.
pub type Result<T> = std::result::Result<T, StorageError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn io_error_converts() {
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file gone");
        let storage_err: StorageError = io_err.into();
        assert!(matches!(storage_err, StorageError::Io(_)));
    }
}
