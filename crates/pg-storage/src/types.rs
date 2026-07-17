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
pub struct FrameId(pub usize);

impl FrameId {
    /// The invalid frame ID.
    pub const INVALID: FrameId = FrameId(usize::MAX);
}

/// A database object identifier (table, type, index, etc.).
///
/// `Oid(0)` is reserved and never allocated. User OIDs start at 16384
/// (matching PostgreSQL's user OID range). System OIDs are in the range
/// `[1, 9999]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Oid(pub u64);

impl Oid {
    /// The reserved OID 0 (never allocated).
    pub const INVALID: Oid = Oid(0);

    /// The first user OID. System OIDs occupy `[1, 9999]`.
    pub const FIRST_USER: Oid = Oid(16384);

    /// Return true if this OID is in the system range `[1, 9999]`.
    pub fn is_system(&self) -> bool {
        self.0 > 0 && self.0 < Self::FIRST_USER.0
    }
}

impl fmt::Display for Oid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Oid({})", self.0)
    }
}

/// A tuple identifier: (page_id, slot_id).
///
/// `repr(C)` guarantees field order (`page_id` first, then `slot_id`) for
/// predictable encoding. Rust's default alignment rules pad the in-memory
/// struct to 16 bytes (8-byte PageId + 2-byte slot + 6-byte padding to the
/// 8-byte alignment boundary). The useful payload is 10 bytes.
///
/// On-disk layout inside tuple headers: 12 bytes (8-byte PageId + 2-byte slot +
/// 2-byte padding for 8-byte alignment), matching the M2 tuple header `t_ctid`
/// field layout. Encoding is done manually, not via `repr(C)` memcpy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(C)]
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

/// Align `n` up to the next multiple of `align`.
///
/// `align` must be a power of two.
pub(crate) fn align_up(n: usize, align: usize) -> usize {
    assert!(align.is_power_of_two());
    (n + align - 1) & !(align - 1)
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

    #[test]
    fn oid_system_range() {
        assert!(Oid(1259).is_system());
        assert!(Oid(9999).is_system());
        assert!(!Oid(16384).is_system());
        assert!(!Oid::INVALID.is_system());
        assert!(Oid(10000).is_system());
    }

    #[test]
    fn oid_ordering() {
        assert!(Oid::INVALID < Oid::FIRST_USER);
        assert!(Oid(1259) < Oid(16384));
    }

    #[test]
    fn oid_display() {
        assert_eq!(format!("{}", Oid(1259)), "Oid(1259)");
    }

    #[test]
    fn oid_serde_round_trip() {
        let oid = Oid(16384);
        let encoded = bincode::serde::encode_to_vec(oid, bincode::config::standard()).unwrap();
        let (decoded, _): (Oid, usize) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        assert_eq!(oid, decoded);
    }

    #[test]
    fn tid_layout() {
        // repr(C) guarantees field order: page_id (8B) then slot_id (2B).
        // Rust rounds struct size up to the alignment of the largest field (8B),
        // so the actual in-memory size is 16 bytes. The useful payload is 10
        // bytes; the on-disk tuple-header layout manually encodes as 12 bytes
        // (8B PageId + 2B slot + 2B padding).
        assert_eq!(std::mem::size_of::<Tid>(), 16);
        assert_eq!(std::mem::align_of::<Tid>(), 8);
    }
}
