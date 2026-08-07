//! Heap access method error types.

use thiserror::Error;

/// Errors returned by the heap access method.
#[derive(Debug, Error)]
pub enum HeapError {
    /// The page does not have enough contiguous free space for the tuple
    /// (tuple bytes plus, when no `Unused` slot can be recycled, one
    /// additional 4-byte line pointer).
    #[error("page full: need {needed} bytes, have {available}")]
    PageFull {
        /// Bytes required (tuple length, plus a line pointer if a new slot
        /// must be allocated).
        needed: usize,
        /// Contiguous free bytes between `pd_lower` and `pd_upper`.
        available: usize,
    },

    /// The slot index is out of range, or the slot does not reference a live
    /// (`Normal`) tuple where one was required.
    #[error("invalid slot {0}")]
    InvalidSlot(u16),

    /// The tuple is too large to ever fit on a page of this size.
    #[error("tuple too large: {0} bytes")]
    TupleTooLarge(usize),

    /// On-disk bytes are malformed (bad varlena header, truncated tuple,
    /// inconsistent header fields, etc.).
    #[error("corrupted data: {0}")]
    Corrupted(String),

    /// A caller supplied invalid arguments (schema/value length mismatch,
    /// datum type does not match the column, too many columns, oversized
    /// varlena). This is a programming error at the call site, distinct from
    /// [`HeapError::Corrupted`], which describes bad on-disk bytes.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// No live tuple exists at the given TID (out-of-range page/slot, or the
    /// slot holds a dead/unused line pointer). Raised by update/delete when
    /// the target row cannot be found.
    #[error("tuple not found at {0:?}")]
    TupleNotFound(pg_storage::types::Tid),

    /// The tuple at the given TID was deleted or updated by another
    /// transaction that has since COMMITTED (M2c Stage P, §9.1 step 3 of the
    /// row-lock protocol). Distinct from [`HeapError::TupleNotFound`]: the
    /// row DID exist when the caller's snapshot was taken, but the version
    /// addressed is dead — SQL layers map this to "tuple concurrently
    /// updated" (snapshot-isolation write conflict), not to "row does not
    /// exist". Callers may retry with a fresh snapshot.
    ///
    /// Only produced when the AM has a row waiter installed; the legacy
    /// no-waiter mode keeps reporting this condition as `TupleNotFound`
    /// (see `HeapAM::row_lock_gate`).
    #[error("tuple at {0:?} was concurrently updated or deleted")]
    TupleConcurrentlyUpdated(pg_storage::types::Tid),

    /// The deadlock detector (M2c Stage R, tech-selection §9.3) interrupted
    /// this transaction's row-lock wait: it was chosen as the victim of a
    /// wait-for cycle. The current statement fails; the caller must abort
    /// the transaction (auto-commit does so on any statement error). Like
    /// PostgreSQL's `deadlock detected`, the error is safe to retry as a
    /// fresh transaction.
    #[error("deadlock detected")]
    DeadlockVictim,

    /// A lower-level storage engine operation failed (buffer pool, WAL, page
    /// allocator).
    #[error("storage error: {0}")]
    Storage(#[from] pg_storage::error::StorageError),
}

/// A convenient type alias for heap-AM results.
pub type Result<T> = std::result::Result<T, HeapError>;
