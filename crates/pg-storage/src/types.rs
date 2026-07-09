//! Core storage identifiers and newtypes used across the storage engine.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::hash::Hash;

/// Size of a database page in bytes.
///
/// This is a compile-time constant controlled by Cargo features:
/// - Default: 8 KB (matches PostgreSQL)
/// - `page-size-16k`: 16 KB
///
/// The value must remain uniform for the lifetime of a database; changing it
/// requires reinitializing the data directory.
#[cfg(not(feature = "page-size-16k"))]
pub const PAGE_SIZE: usize = 8192;

/// Size of a database page in bytes.
///
/// This is a compile-time constant controlled by Cargo features:
/// - Default: 8 KB (matches PostgreSQL)
/// - `page-size-16k`: 16 KB
///
/// The value must remain uniform for the lifetime of a database; changing it
/// requires reinitializing the data directory.
#[cfg(feature = "page-size-16k")]
pub const PAGE_SIZE: usize = 16384;

// Compile-time invariants for PAGE_SIZE.
const _: () = assert!(
    PAGE_SIZE.is_power_of_two(),
    "PAGE_SIZE must be a power of two"
);
const _: () = assert!(PAGE_SIZE >= 4096, "PAGE_SIZE must be at least 4096");

/// WAL segment size in bytes.
pub const WAL_SEGMENT_SIZE: u64 = 16 * 1024 * 1024;

/// Alignment requirement for WAL records (and therefore LSN values).
pub const LSN_ALIGNMENT: u64 = 8;

/// A physical page identifier.
///
/// `PageId(0)` is reserved and never allocated. Page IDs start at 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PageId(pub u64);

impl PageId {
    /// The reserved page ID 0 (never allocated).
    pub const INVALID: PageId = PageId(0);

    /// The first valid page ID.
    pub const FIRST: PageId = PageId(1);
}

impl fmt::Display for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PageId({})", self.0)
    }
}

/// A Log Sequence Number.
///
/// LSNs are global byte offsets into the WAL stream. They are always a
/// multiple of [`LSN_ALIGNMENT`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Lsn(pub u64);

impl Lsn {
    /// The invalid LSN (0).
    pub const INVALID: Lsn = Lsn(0);

    /// The first valid LSN. Must be aligned to [`LSN_ALIGNMENT`].
    pub const FIRST: Lsn = Lsn(LSN_ALIGNMENT);

    /// Return true if this LSN is not [`Lsn::INVALID`].
    pub fn is_valid(&self) -> bool {
        self.0 != 0
    }

    /// Compute the WAL segment ID that contains this LSN.
    pub fn segment_id(&self, segment_size: u64) -> u64 {
        self.0 / segment_size
    }

    /// Compute the offset within the WAL segment that contains this LSN.
    pub fn segment_offset(&self, segment_size: u64) -> u64 {
        self.0 % segment_size
    }

    /// Return the LSN that immediately follows a record of `record_size`
    /// bytes. Panics in debug mode if `record_size` is not aligned.
    pub fn advance(&self, record_size: u64) -> Lsn {
        debug_assert!(
            record_size % LSN_ALIGNMENT == 0,
            "record_size must be a multiple of {LSN_ALIGNMENT}"
        );
        Lsn(self.0 + record_size)
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Lsn({})", self.0)
    }
}

/// A transaction identifier.
///
/// 64-bit transaction IDs avoid the XID wraparound problem that PostgreSQL
/// faces with 32-bit XIDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TxnId(pub u64);

impl TxnId {
    /// The invalid transaction ID (0).
    pub const INVALID: TxnId = TxnId(0);

    /// The first valid transaction ID.
    pub const FIRST: TxnId = TxnId(1);
}

impl fmt::Display for TxnId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TxnId({})", self.0)
    }
}

/// A frame identifier inside the buffer pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FrameId(pub u32);

impl FrameId {
    /// The invalid frame ID.
    pub const INVALID: FrameId = FrameId(u32::MAX);
}

/// A tuple identifier: (page_id, slot_id).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Tid {
    /// The page containing the tuple.
    pub page_id: PageId,
    /// The slot index within the page.
    pub slot_id: u16,
}

impl fmt::Display for Tid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Tid({}.{})", self.page_id.0, self.slot_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_segment_math() {
        let lsn = Lsn(42_000_000);
        assert_eq!(lsn.segment_id(WAL_SEGMENT_SIZE), 2);
        assert_eq!(lsn.segment_offset(WAL_SEGMENT_SIZE), 8_445_568);
    }

    #[test]
    fn lsn_advance_requires_alignment() {
        let lsn = Lsn::FIRST;
        let next = lsn.advance(32);
        assert_eq!(next.0, 8 + 32);
    }

    #[test]
    #[should_panic]
    fn lsn_advance_panics_on_unaligned_size() {
        let lsn = Lsn::FIRST;
        let _ = lsn.advance(5);
    }
}
