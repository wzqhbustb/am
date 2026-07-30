//! B+Tree page format (tech-selection §13.1).
//!
//! A B+Tree page reuses the heap slotted-page layout
//! ([`pg_am_heap::SlottedPage`]) with a fixed 16-byte special space:
//!
//! ```text
//! │ pd_special+0..8  │ btpo_prev: left sibling  (PageId, LE; 0 = none) │
//! │ pd_special+8..16 │ btpo_next: right sibling (PageId, LE; 0 = none) │
//! ```
//!
//! `btpo_level` and `btpo_flags` live in `pd_flags` (the special space stays
//! 16 bytes, §13.1):
//!
//! - `pd_flags` bits 8..11  = `btpo_level` (0 = leaf)
//! - `pd_flags` bits 12..15 = `btpo_flags` ([`BTREE_FLAG_LEAF`],
//!   [`BTREE_FLAG_ROOT`], [`BTREE_FLAG_DELETED`],
//!   [`BTREE_FLAG_SPLIT_INCOMPLETE`])
//!
//! # Entry encoding (§13.1, §7.3)
//!
//! Index entries carry no MVCC header — the heap's 64-byte `TupleHeader` is
//! not used. Entries are manually LE-encoded with the fixed-size trailer
//! last, so the variable-length key needs no length prefix (matching the
//! `BTreeInsertRecord` payload contract in `pg-storage`):
//!
//! - leaf entry:     `key_bytes ++ tid(page_id: u64 LE, slot_id: u16 LE)` (10B tail)
//! - internal entry: `key_bytes ++ child_page_id(u64 LE)`                 (8B tail)
//! - meta record:    `root_page_id(u64 LE) ++ tree_level(u16 LE)`         (10B total)
//!
//! Keys may repeat; the full sort order of a page is `(key_bytes, trailer)`
//! lexicographic, so duplicate keys are disambiguated by the heap TID (leaf)
//! or child page id (internal).
//!
//! # Sorted LP array
//!
//! Unlike the heap (append-only LP array), a B+Tree page keeps its line
//! pointers **sorted by entry** so binary search works directly on slot
//! numbers. [`BtreePage::insert_entry_at`] / [`BtreePage::remove_entry_at`]
//! shift the LP array; [`BtreePage::truncate_slots`] drops the tail of the
//! array (split copy). Tuple bytes of removed entries are left as dead space
//! between `pd_upper` and the live region, exactly like PG's `pd_lower`
//! truncation; the space is reclaimed by the next split.
//!
//! All readers validate page geometry and return [`BTreeError::Corrupted`]
//! on inconsistency — corrupted page bytes must never cause a panic (Stage G
//! hardening style).

use pg_am_heap::line_pointer::{LinePointer, LpFlags, LINE_POINTER_SIZE};
use pg_am_heap::slotted_page::{debug_assert_invariants, SlottedPage, HEAP_SPECIAL_SIZE};
use pg_storage::page::{PageHeader, PAGE_HEADER_SIZE};
use pg_storage::types::{PageId, Tid, PAGE_SIZE};

use crate::error::{BTreeError, Result};

/// Special-space size of every B+Tree page (meta pages included), in bytes.
///
/// Identical to the heap's special geometry so the same slotted-page
/// primitives apply unchanged (§13.1 "复用 slotted page 布局").
pub const BTREE_SPECIAL_SIZE: usize = HEAP_SPECIAL_SIZE;

/// `btpo_flags`: page is a leaf (level 0).
pub const BTREE_FLAG_LEAF: u8 = 0x1;
/// `btpo_flags`: page is the tree root. Advisory in M2b — the meta page is
/// authoritative for root identity — but maintained so M2c can rely on it.
pub const BTREE_FLAG_ROOT: u8 = 0x2;
/// `btpo_flags`: page is deleted (half-dead in Blink terms). Never set in
/// M2b (no page merge); reserved by §13.1.
pub const BTREE_FLAG_DELETED: u8 = 0x4;
/// `btpo_flags`: the page's split started (`BTreeSplitPrepare`) but its
/// downlink has not been committed (`BTreeSplitCommit`). Cleared by Commit.
pub const BTREE_FLAG_SPLIT_INCOMPLETE: u8 = 0x8;

const LEVEL_SHIFT: u16 = 8;
const FLAGS_SHIFT: u16 = 12;

/// Byte length of the leaf-entry trailer (`page_id: u64 + slot_id: u16`).
pub const LEAF_TRAILER_SIZE: usize = 10;
/// Byte length of the internal-entry trailer (`child_page_id: u64`).
pub const INTERNAL_TRAILER_SIZE: usize = 8;
/// Byte length of a meta-page record (`root_page_id: u64 + tree_level: u16`).
pub const META_RECORD_SIZE: usize = 10;

/// Slotted-page operations with B+Tree semantics on a raw page buffer.
///
/// All methods are associated functions taking the page buffer explicitly,
/// mirroring [`pg_am_heap::SlottedPage`]; there is no owned state.
pub struct BtreePage;

impl BtreePage {
    /// Initialize a fresh page as a B+Tree page with the given `btpo_level`
    /// and `btpo_flags`, zeroing all prior content.
    pub fn init(page: &mut [u8; PAGE_SIZE], level: u8, flags: u8) {
        SlottedPage::init_with_special(page, BTREE_SPECIAL_SIZE);
        Self::set_level(page, level);
        Self::set_flags(page, flags);
    }

    /// Initialize the page only if it has never been initialized
    /// (`pd_upper == 0` uniquely identifies an all-zero page, same trick as
    /// the heap's `init_if_fresh_with_special`). Returns `true` if it did.
    pub fn init_if_fresh(page: &mut [u8; PAGE_SIZE], level: u8, flags: u8) -> bool {
        if SlottedPage::header(page).pd_upper == 0 {
            Self::init(page, level, flags);
            return true;
        }
        false
    }

    /// Decode the header and validate the geometry every B+Tree page must
    /// satisfy: the heap slotted invariants plus exactly
    /// [`BTREE_SPECIAL_SIZE`] bytes of special space.
    fn checked_header(page: &[u8; PAGE_SIZE]) -> Result<PageHeader> {
        let header = SlottedPage::header(page);
        let (lower, upper, special) = (
            header.pd_lower as usize,
            header.pd_upper as usize,
            header.pd_special as usize,
        );
        if lower < PAGE_HEADER_SIZE
            || (lower - PAGE_HEADER_SIZE) % LINE_POINTER_SIZE != 0
            || lower > upper
            || upper > special
            || special != PAGE_SIZE - BTREE_SPECIAL_SIZE
        {
            return Err(BTreeError::Corrupted(format!(
                "bad btree page geometry: pd_lower={lower} pd_upper={upper} pd_special={special}"
            )));
        }
        Ok(header)
    }

    /// `btpo_level` of the page (0 = leaf).
    pub fn level(page: &[u8; PAGE_SIZE]) -> Result<u8> {
        let header = Self::checked_header(page)?;
        Ok(((header.pd_flags >> LEVEL_SHIFT) & 0x0F) as u8)
    }

    /// `btpo_flags` of the page.
    pub fn flags(page: &[u8; PAGE_SIZE]) -> Result<u8> {
        let header = Self::checked_header(page)?;
        Ok(((header.pd_flags >> FLAGS_SHIFT) & 0x0F) as u8)
    }

    /// Write `btpo_level` (bits 8..11 of `pd_flags`), preserving the rest.
    pub fn set_level(page: &mut [u8; PAGE_SIZE], level: u8) {
        debug_assert!(level <= 0x0F, "btpo_level is a 4-bit field (§13.1)");
        let raw = u16::from_le_bytes(page[12..14].try_into().unwrap());
        let raw = (raw & !(0x0F << LEVEL_SHIFT)) | ((level as u16) << LEVEL_SHIFT);
        page[12..14].copy_from_slice(&raw.to_le_bytes());
    }

    /// Write `btpo_flags` (bits 12..15 of `pd_flags`), preserving the rest.
    pub fn set_flags(page: &mut [u8; PAGE_SIZE], flags: u8) {
        debug_assert!(flags <= 0x0F, "btpo_flags is a 4-bit field (§13.1)");
        let raw = u16::from_le_bytes(page[12..14].try_into().unwrap());
        let raw = (raw & !(0x0F << FLAGS_SHIFT)) | ((flags as u16) << FLAGS_SHIFT);
        page[12..14].copy_from_slice(&raw.to_le_bytes());
    }

    /// Set or clear one `btpo_flags` bit, preserving the others.
    ///
    /// Returns [`BTreeError::Corrupted`] if the page is not a well-formed
    /// B+Tree page — silently defaulting the flags to 0 and writing them
    /// back would zero out any flags already present (module rule: corrupted
    /// bytes are never silently absorbed).
    pub fn set_flag(page: &mut [u8; PAGE_SIZE], flag: u8, on: bool) -> Result<()> {
        let mut flags = Self::flags(page)?;
        if on {
            flags |= flag;
        } else {
            flags &= !flag;
        }
        Self::set_flags(page, flags);
        Ok(())
    }

    /// `btpo_prev` (left sibling); [`PageId::INVALID`] means none.
    pub fn prev(page: &[u8; PAGE_SIZE]) -> Result<PageId> {
        let header = Self::checked_header(page)?;
        let off = header.pd_special as usize;
        Ok(PageId(u64::from_le_bytes(
            page[off..off + 8].try_into().unwrap(),
        )))
    }

    /// `btpo_next` (right sibling); [`PageId::INVALID`] means none.
    pub fn next(page: &[u8; PAGE_SIZE]) -> Result<PageId> {
        let header = Self::checked_header(page)?;
        let off = header.pd_special as usize;
        Ok(PageId(u64::from_le_bytes(
            page[off + 8..off + 16].try_into().unwrap(),
        )))
    }

    /// Write `btpo_prev` ([`PageId::INVALID`] clears it to 0).
    pub fn set_prev(page: &mut [u8; PAGE_SIZE], prev: PageId) {
        let off = SlottedPage::header(page).pd_special as usize;
        debug_assert_eq!(off, PAGE_SIZE - BTREE_SPECIAL_SIZE);
        page[off..off + 8].copy_from_slice(&prev.0.to_le_bytes());
    }

    /// Write `btpo_next` ([`PageId::INVALID`] clears it to 0).
    pub fn set_next(page: &mut [u8; PAGE_SIZE], next: PageId) {
        let off = SlottedPage::header(page).pd_special as usize;
        debug_assert_eq!(off, PAGE_SIZE - BTREE_SPECIAL_SIZE);
        page[off + 8..off + 16].copy_from_slice(&next.0.to_le_bytes());
    }

    /// The default `btpo_flags` for a page of `level`: leaves get
    /// [`BTREE_FLAG_LEAF`], internal pages get none.
    pub fn flags_for_level(level: u8) -> u8 {
        if level == 0 {
            BTREE_FLAG_LEAF
        } else {
            0
        }
    }

    /// Insert `entry` at `slot`, shifting the LP array tail one slot right so
    /// the slot order stays sorted by entry. `slot == slot_count` appends.
    ///
    /// Returns [`BTreeError::PageFull`] when the free space cannot hold the
    /// entry plus one line pointer, and [`BTreeError::Corrupted`] when `slot`
    /// is beyond the current slot count (a torn LP array must hard-fail, not
    /// silently scribble, §11.6).
    pub fn insert_entry_at(page: &mut [u8; PAGE_SIZE], slot: u16, entry: &[u8]) -> Result<()> {
        let header = Self::checked_header(page)?;
        if entry.is_empty() {
            return Err(BTreeError::InvalidArgument(
                "cannot insert an empty index entry".to_string(),
            ));
        }
        let count = (header.pd_lower as usize - PAGE_HEADER_SIZE) / LINE_POINTER_SIZE;
        if slot as usize > count {
            return Err(BTreeError::Corrupted(format!(
                "insert slot {slot} beyond slot count {count}"
            )));
        }
        let max_entry =
            (PAGE_SIZE - BTREE_SPECIAL_SIZE).saturating_sub(PAGE_HEADER_SIZE + LINE_POINTER_SIZE);
        if entry.len() > max_entry {
            return Err(BTreeError::KeyTooLarge(entry.len()));
        }
        let needed = entry.len() + LINE_POINTER_SIZE;
        let free = (header.pd_upper - header.pd_lower) as usize;
        if free < needed {
            return Err(BTreeError::PageFull {
                needed,
                available: free,
            });
        }

        // Place the entry bytes at the top of the free space.
        let new_upper = header.pd_upper as usize - entry.len();
        page[new_upper..new_upper + entry.len()].copy_from_slice(entry);

        // Shift LPs [slot, count) one position right and write the new one.
        let lp_start = PAGE_HEADER_SIZE + slot as usize * LINE_POINTER_SIZE;
        let lp_end = PAGE_HEADER_SIZE + count * LINE_POINTER_SIZE;
        page.copy_within(lp_start..lp_end, lp_start + LINE_POINTER_SIZE);
        let lp = LinePointer::new(new_upper as u16, LpFlags::Normal, entry.len() as u16);
        page[lp_start..lp_start + LINE_POINTER_SIZE].copy_from_slice(&lp.to_le_bytes());

        page[14..16].copy_from_slice(&(header.pd_lower + LINE_POINTER_SIZE as u16).to_le_bytes());
        page[16..18].copy_from_slice(&(new_upper as u16).to_le_bytes());

        if cfg!(debug_assertions) {
            debug_assert_invariants(page);
        }
        Ok(())
    }

    /// Remove the entry at `slot`, shifting the LP array tail one slot left.
    /// The entry's bytes are left as dead space (reclaimed by the next
    /// split); only the LP array shrinks.
    pub fn remove_entry_at(page: &mut [u8; PAGE_SIZE], slot: u16) -> Result<()> {
        let header = Self::checked_header(page)?;
        let count = (header.pd_lower as usize - PAGE_HEADER_SIZE) / LINE_POINTER_SIZE;
        if slot as usize >= count {
            return Err(BTreeError::Corrupted(format!(
                "remove slot {slot} beyond slot count {count}"
            )));
        }
        let from = PAGE_HEADER_SIZE + (slot as usize + 1) * LINE_POINTER_SIZE;
        let to = PAGE_HEADER_SIZE + count * LINE_POINTER_SIZE;
        page.copy_within(from..to, from - LINE_POINTER_SIZE);
        page[14..16].copy_from_slice(&(header.pd_lower - LINE_POINTER_SIZE as u16).to_le_bytes());
        if cfg!(debug_assertions) {
            debug_assert_invariants(page);
        }
        Ok(())
    }

    /// Drop every slot at index `>= keep` by shrinking the LP array (split
    /// copy step: the entries were moved to the right sibling). The tuple
    /// bytes stay as dead space, so the moved content remains readable from
    /// the raw page until overwritten — exactly what the `BTreeSplitCopy`
    /// redo recomputation relies on never needing.
    pub fn truncate_slots(page: &mut [u8; PAGE_SIZE], keep: u16) -> Result<()> {
        let header = Self::checked_header(page)?;
        let count = (header.pd_lower as usize - PAGE_HEADER_SIZE) / LINE_POINTER_SIZE;
        if keep as usize > count {
            return Err(BTreeError::Corrupted(format!(
                "truncate to {keep} beyond slot count {count}"
            )));
        }
        let new_lower = (PAGE_HEADER_SIZE + keep as usize * LINE_POINTER_SIZE) as u16;
        page[14..16].copy_from_slice(&new_lower.to_le_bytes());
        if cfg!(debug_assertions) {
            debug_assert_invariants(page);
        }
        Ok(())
    }

    /// Apply the left-page half of a `BTreeSplitPrepare` (§13.3 step 1): mark
    /// the page split-incomplete and link it to its new right sibling.
    pub fn apply_prepare_left(page: &mut [u8; PAGE_SIZE], new_right: PageId) -> Result<()> {
        Self::checked_header(page)?;
        // A fresh, all-zero page passes the geometry check but is not an
        // initialized B+Tree page — refuse to write flags onto it (symmetric
        // with the redo path's `pd_upper == 0` guard).
        if SlottedPage::header(page).pd_upper == 0 {
            return Err(BTreeError::Corrupted(
                "apply_prepare_left on an uninitialized page".to_string(),
            ));
        }
        Self::set_flag(page, BTREE_FLAG_SPLIT_INCOMPLETE, true)?;
        Self::set_next(page, new_right);
        Ok(())
    }

    /// Initialize `page` as the new right sibling of a split (§13.3 step 1
    /// redo semantics): a full re-initialization — any previous tenant's
    /// bytes are overwritten. Safe because redo guards on
    /// `pd_lsn < record.lsn`, and every earlier write to a recycled page has
    /// a lower LSN than the `BTreeSplitPrepare` record.
    pub fn init_right_page(
        page: &mut [u8; PAGE_SIZE],
        left: PageId,
        left_old_next: PageId,
        level: u8,
    ) {
        Self::init(page, level, Self::flags_for_level(level));
        Self::set_prev(page, left);
        Self::set_next(page, left_old_next);
    }

    /// Apply the left-page half of a `BTreeSplitCommit` (§13.3 step 3):
    /// clear `SPLIT_INCOMPLETE`, and clear `ROOT` — the only split whose left
    /// page carries `ROOT` is a root split, and its Commit installs a new
    /// root. For non-root splits clearing `ROOT` is a no-op.
    pub fn apply_commit_left(page: &mut [u8; PAGE_SIZE]) -> Result<()> {
        Self::checked_header(page)?;
        // Same uninitialized-page guard as `apply_prepare_left`.
        if SlottedPage::header(page).pd_upper == 0 {
            return Err(BTreeError::Corrupted(
                "apply_commit_left on an uninitialized page".to_string(),
            ));
        }
        let flags = Self::flags(page)?;
        Self::set_flags(
            page,
            flags & !(BTREE_FLAG_SPLIT_INCOMPLETE | BTREE_FLAG_ROOT),
        );
        Ok(())
    }
}

/// Encode a leaf entry: `key_bytes ++ tid(10B LE trailer)`.
pub fn encode_leaf_entry(key: &[u8], tid: Tid) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + LEAF_TRAILER_SIZE);
    out.extend_from_slice(key);
    out.extend_from_slice(&tid.page_id.0.to_le_bytes());
    out.extend_from_slice(&tid.slot_id.to_le_bytes());
    out
}

/// Decode a leaf entry into its key bytes and heap TID.
pub fn decode_leaf_entry(bytes: &[u8]) -> Result<(&[u8], Tid)> {
    if bytes.len() < LEAF_TRAILER_SIZE {
        return Err(BTreeError::Corrupted(format!(
            "leaf entry of {} bytes is shorter than the {}-byte trailer",
            bytes.len(),
            LEAF_TRAILER_SIZE
        )));
    }
    let (key, tail) = bytes.split_at(bytes.len() - LEAF_TRAILER_SIZE);
    let tid = Tid {
        page_id: PageId(u64::from_le_bytes(tail[0..8].try_into().unwrap())),
        slot_id: u16::from_le_bytes(tail[8..10].try_into().unwrap()),
    };
    Ok((key, tid))
}

/// Encode an internal entry: `key_bytes ++ child_page_id(8B LE trailer)`.
pub fn encode_internal_entry(key: &[u8], child: PageId) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + INTERNAL_TRAILER_SIZE);
    out.extend_from_slice(key);
    out.extend_from_slice(&child.0.to_le_bytes());
    out
}

/// Decode an internal entry into its key bytes and child page id.
pub fn decode_internal_entry(bytes: &[u8]) -> Result<(&[u8], PageId)> {
    if bytes.len() < INTERNAL_TRAILER_SIZE {
        return Err(BTreeError::Corrupted(format!(
            "internal entry of {} bytes is shorter than the {}-byte trailer",
            bytes.len(),
            INTERNAL_TRAILER_SIZE
        )));
    }
    let (key, tail) = bytes.split_at(bytes.len() - INTERNAL_TRAILER_SIZE);
    Ok((key, PageId(u64::from_le_bytes(tail.try_into().unwrap()))))
}

/// Encode a meta-page record: `root_page_id(u64 LE) ++ tree_level(u16 LE)`.
pub fn encode_meta_record(root: PageId, tree_level: u16) -> Vec<u8> {
    let mut out = Vec::with_capacity(META_RECORD_SIZE);
    out.extend_from_slice(&root.0.to_le_bytes());
    out.extend_from_slice(&tree_level.to_le_bytes());
    out
}

/// Decode a meta-page record into `(root_page_id, tree_level)`.
pub fn decode_meta_record(bytes: &[u8]) -> Result<(PageId, u16)> {
    if bytes.len() != META_RECORD_SIZE {
        return Err(BTreeError::Corrupted(format!(
            "meta record of {} bytes, expected {META_RECORD_SIZE}",
            bytes.len()
        )));
    }
    let root = PageId(u64::from_le_bytes(bytes[0..8].try_into().unwrap()));
    let level = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
    Ok((root, level))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tid(page: u64, slot: u16) -> Tid {
        Tid {
            page_id: PageId(page),
            slot_id: slot,
        }
    }

    fn fresh_page(level: u8, flags: u8) -> [u8; PAGE_SIZE] {
        let mut page = [0u8; PAGE_SIZE];
        BtreePage::init(&mut page, level, flags);
        page
    }

    #[test]
    fn init_sets_level_and_flags() {
        let page = fresh_page(3, BTREE_FLAG_ROOT);
        assert_eq!(BtreePage::level(&page).unwrap(), 3);
        assert_eq!(BtreePage::flags(&page).unwrap(), BTREE_FLAG_ROOT);
        assert_eq!(SlottedPage::slot_count(&page), 0);
        assert_eq!(BtreePage::prev(&page).unwrap(), PageId::INVALID);
        assert_eq!(BtreePage::next(&page).unwrap(), PageId::INVALID);
    }

    #[test]
    fn level_and_flags_share_pd_flags_byte_space() {
        let mut page = fresh_page(0, 0);
        BtreePage::set_level(&mut page, 15);
        BtreePage::set_flags(&mut page, 0x0F);
        assert_eq!(BtreePage::level(&page).unwrap(), 15);
        assert_eq!(BtreePage::flags(&page).unwrap(), 0x0F);
        // The low byte of pd_flags is untouched by both nibbles.
        assert_eq!(page[12], 0);
    }

    #[test]
    fn set_flag_toggles_single_bits() {
        let mut page = fresh_page(0, BTREE_FLAG_LEAF);
        BtreePage::set_flag(&mut page, BTREE_FLAG_SPLIT_INCOMPLETE, true).unwrap();
        assert_eq!(
            BtreePage::flags(&page).unwrap(),
            BTREE_FLAG_LEAF | BTREE_FLAG_SPLIT_INCOMPLETE
        );
        BtreePage::set_flag(&mut page, BTREE_FLAG_LEAF, false).unwrap();
        assert_eq!(
            BtreePage::flags(&page).unwrap(),
            BTREE_FLAG_SPLIT_INCOMPLETE
        );
    }

    #[test]
    fn set_flag_rejects_corrupt_page_instead_of_zeroing_flags() {
        // A non-btree page (special_size = 0, no chain geometry) must error,
        // not silently have flags=0 written back over it.
        let mut page = [0u8; PAGE_SIZE];
        pg_am_heap::SlottedPage::init(&mut page);
        assert!(BtreePage::set_flag(&mut page, BTREE_FLAG_SPLIT_INCOMPLETE, true).is_err());
    }

    #[test]
    fn apply_prepare_and_commit_left_reject_uninitialized_page() {
        // A fresh, all-zero page passes the geometry check but is not an
        // initialized B+Tree page — both apply paths must refuse loudly.
        let mut page = [0u8; PAGE_SIZE];
        assert!(BtreePage::apply_prepare_left(&mut page, PageId(9)).is_err());
        assert!(BtreePage::apply_commit_left(&mut page).is_err());
    }

    #[test]
    fn sibling_pointers_round_trip() {
        let mut page = fresh_page(0, BTREE_FLAG_LEAF);
        BtreePage::set_prev(&mut page, PageId(11));
        BtreePage::set_next(&mut page, PageId(22));
        assert_eq!(BtreePage::prev(&page).unwrap(), PageId(11));
        assert_eq!(BtreePage::next(&page).unwrap(), PageId(22));
    }

    #[test]
    fn bad_geometry_is_corrupted_not_panic() {
        // special-less heap page: pd_special == PAGE_SIZE.
        let mut page = [0u8; PAGE_SIZE];
        SlottedPage::init(&mut page);
        assert!(matches!(
            BtreePage::level(&page),
            Err(BTreeError::Corrupted(_))
        ));
        assert!(matches!(
            BtreePage::next(&page),
            Err(BTreeError::Corrupted(_))
        ));

        // pd_lower beyond pd_upper.
        let mut page = fresh_page(0, BTREE_FLAG_LEAF);
        page[14..16].copy_from_slice(&8000u16.to_le_bytes());
        page[16..18].copy_from_slice(&100u16.to_le_bytes());
        assert!(matches!(
            BtreePage::flags(&page),
            Err(BTreeError::Corrupted(_))
        ));
    }

    #[test]
    fn insert_entry_at_keeps_slots_sorted() {
        let mut page = fresh_page(0, BTREE_FLAG_LEAF);
        let e1 = encode_leaf_entry(b"a", tid(1, 0));
        let e3 = encode_leaf_entry(b"c", tid(3, 0));
        let e2 = encode_leaf_entry(b"b", tid(2, 0));
        BtreePage::insert_entry_at(&mut page, 0, &e3).unwrap();
        BtreePage::insert_entry_at(&mut page, 0, &e1).unwrap();
        BtreePage::insert_entry_at(&mut page, 1, &e2).unwrap();
        assert_eq!(SlottedPage::slot_count(&page), 3);
        for (slot, expect) in [(0u16, &e1), (1, &e2), (2, &e3)] {
            assert_eq!(
                SlottedPage::tuple(&page, slot).unwrap(),
                Some(expect.as_slice())
            );
        }
    }

    #[test]
    fn insert_beyond_slot_count_is_corrupted() {
        let mut page = fresh_page(0, BTREE_FLAG_LEAF);
        let e = encode_leaf_entry(b"a", tid(1, 0));
        assert!(matches!(
            BtreePage::insert_entry_at(&mut page, 1, &e),
            Err(BTreeError::Corrupted(_))
        ));
    }

    #[test]
    fn page_full_is_reported() {
        let mut page = fresh_page(0, BTREE_FLAG_LEAF);
        let e = encode_leaf_entry(&[0xAA; 100], tid(1, 0));
        loop {
            let slot = SlottedPage::slot_count(&page) as u16;
            if BtreePage::insert_entry_at(&mut page, slot, &e).is_err() {
                break;
            }
        }
        assert!(matches!(
            BtreePage::insert_entry_at(&mut page, 0, &e),
            Err(BTreeError::PageFull { .. })
        ));
    }

    #[test]
    fn remove_entry_at_shifts_tail() {
        let mut page = fresh_page(0, BTREE_FLAG_LEAF);
        for (i, k) in [b"a", b"b", b"c"].iter().enumerate() {
            let e = encode_leaf_entry(*k, tid(i as u64, 0));
            BtreePage::insert_entry_at(&mut page, i as u16, &e).unwrap();
        }
        BtreePage::remove_entry_at(&mut page, 0).unwrap();
        assert_eq!(SlottedPage::slot_count(&page), 2);
        let (k0, _) = decode_leaf_entry(SlottedPage::tuple(&page, 0).unwrap().unwrap()).unwrap();
        let (k1, _) = decode_leaf_entry(SlottedPage::tuple(&page, 1).unwrap().unwrap()).unwrap();
        assert_eq!(k0, b"b");
        assert_eq!(k1, b"c");
    }

    #[test]
    fn truncate_slots_drops_tail() {
        let mut page = fresh_page(0, BTREE_FLAG_LEAF);
        for i in 0..4u16 {
            let e = encode_leaf_entry(&[i as u8], tid(i as u64, 0));
            BtreePage::insert_entry_at(&mut page, i, &e).unwrap();
        }
        BtreePage::truncate_slots(&mut page, 2).unwrap();
        assert_eq!(SlottedPage::slot_count(&page), 2);
        assert!(matches!(
            BtreePage::truncate_slots(&mut page, 5),
            Err(BTreeError::Corrupted(_))
        ));
    }

    #[test]
    fn entry_codecs_round_trip() {
        let leaf = encode_leaf_entry(b"key-bytes", tid(0x0102_0304_0506_0708, 42));
        let (k, t) = decode_leaf_entry(&leaf).unwrap();
        assert_eq!(k, b"key-bytes");
        assert_eq!(t, tid(0x0102_0304_0506_0708, 42));

        let internal = encode_internal_entry(b"sep", PageId(999));
        let (k, child) = decode_internal_entry(&internal).unwrap();
        assert_eq!(k, b"sep");
        assert_eq!(child, PageId(999));

        assert!(decode_leaf_entry(&[0u8; 9]).is_err());
        assert!(decode_internal_entry(&[0u8; 7]).is_err());
    }

    #[test]
    fn meta_record_round_trip() {
        let rec = encode_meta_record(PageId(77), 3);
        assert_eq!(decode_meta_record(&rec).unwrap(), (PageId(77), 3));
        assert!(decode_meta_record(&rec[..9]).is_err());
    }

    #[test]
    fn prepare_and_commit_left_transformations() {
        let mut page = fresh_page(0, BTREE_FLAG_LEAF | BTREE_FLAG_ROOT);
        BtreePage::apply_prepare_left(&mut page, PageId(55)).unwrap();
        assert_eq!(BtreePage::next(&page).unwrap(), PageId(55));
        assert!(BtreePage::flags(&page).unwrap() & BTREE_FLAG_SPLIT_INCOMPLETE != 0);
        BtreePage::apply_commit_left(&mut page).unwrap();
        // Commit clears SPLIT_INCOMPLETE *and* ROOT (root-split case).
        assert_eq!(BtreePage::flags(&page).unwrap(), BTREE_FLAG_LEAF);
    }
}
