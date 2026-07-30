//! B+Tree access method error types.

use thiserror::Error;

/// Errors returned by the B+Tree access method.
#[derive(Debug, Error)]
pub enum BTreeError {
    /// A page does not have enough room for an entry, even after reclaiming
    /// the dead space left behind by splits and deletes. Raised by the split
    /// logic when an entry can never fit alongside the minimum occupancy.
    #[error("page full: need {needed} bytes, have {available}")]
    PageFull {
        /// Bytes required (entry bytes plus one 4-byte line pointer).
        needed: usize,
        /// Contiguous free bytes between `pd_lower` and `pd_upper`.
        available: usize,
    },

    /// The key is too large to ever fit in the index (tech-selection §13.1:
    /// an entry must leave room for the minimum split fan-out, mirroring PG's
    /// index-key size limit of roughly 1/3 of a page).
    #[error("index key too large: {0} bytes")]
    KeyTooLarge(usize),

    /// The exact `(key, tid)` pair being inserted already exists. Index
    /// entries are unique in full `(key, tid)` order even though keys may
    /// repeat.
    #[error("duplicate index entry for key")]
    DuplicateKey,

    /// No entry matches the requested `(key, tid)` pair (delete) or key
    /// (lookup via the trait surface).
    #[error("index entry not found")]
    EntryNotFound,

    /// On-disk bytes are malformed (bad page geometry, truncated entry,
    /// meta-page record of the wrong length, sibling-chain cycle, etc.).
    /// Corrupted page bytes must never cause a panic (Stage G hardening
    /// style).
    #[error("corrupted data: {0}")]
    Corrupted(String),

    /// A caller supplied invalid arguments (unsupported key type, empty key,
    /// wrong column count, unexpected `out_tid`).
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// The operation cannot be expressed through the generic AccessMethod
    /// surface (see the method docs for the native alternative).
    #[error("unsupported operation: {0}")]
    Unsupported(String),

    /// A lower-level heap-page primitive failed (slotted-page geometry,
    /// tuple placement). These describe on-disk inconsistency, so they are
    /// preserved rather than collapsed into [`BTreeError::Corrupted`].
    #[error("heap page error: {0}")]
    Heap(#[from] pg_am_heap::HeapError),

    /// A lower-level storage engine operation failed (buffer pool, WAL, page
    /// allocator).
    #[error("storage error: {0}")]
    Storage(#[from] pg_storage::error::StorageError),
}

/// A convenient type alias for B+Tree AM results.
pub type Result<T> = std::result::Result<T, BTreeError>;
