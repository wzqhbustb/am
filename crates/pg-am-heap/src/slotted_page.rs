//! Slotted page operations (tech-selection §二).
//!
//! A heap page is a raw `&mut [u8; PAGE_SIZE]`; this module provides
//! type-state-free functions that interpret it as a slotted page:
//!
//! ```text
//! ┌ PageHeader (32 B, pg_storage::page) ────────────────┐
//! │ LinePointer array  (grows down from offset 32)      │
//! │ ─── free space: pd_lower .. pd_upper ───            │
//! │ Tuple data         (grows up toward the LP array)   │
//! │ Special space      (heap: none, special_size = 0)   │
//! └─────────────────────────────────────────────────────┘
//! ```
//!
//! # Invariants
//!
//! - `PAGE_HEADER_SIZE <= pd_lower <= pd_upper <= pd_special == PAGE_SIZE`.
//!   `pd_lower` and `pd_upper` are the authoritative header fields maintained
//!   by every mutation here; [`debug_assert_invariants`] checks them.
//! - The LP array only ever grows (TID stability, §二 "关键约束"): deletion
//!   marks the LP [`LpFlags::Unused`], and `add_tuple` recycles `Unused`
//!   slots before appending a new LP.
//! - Tuple regions of live (non-`Unused`) LPs lie inside
//!   `[pd_upper, pd_special)` and never overlap.
//!
//! This stage is a pure in-memory format layer: no WAL, no buffer pool, no
//! page compaction (M2a has no vacuum; physical space is reclaimed by page
//! reorganization in a later milestone).

use pg_storage::page::{PageHeader, PAGE_HEADER_SIZE};
use pg_storage::types::PAGE_SIZE;

use crate::error::{HeapError, Result};
use crate::line_pointer::{LinePointer, LpFlags, LINE_POINTER_SIZE};

/// Slotted-page operations on a raw heap page.
///
/// All methods are associated functions taking the page buffer explicitly;
/// there is no owned state.
pub struct SlottedPage;

impl SlottedPage {
    /// Initialize a fresh heap page: 32-byte header, `pd_lower = 32`,
    /// `pd_upper = pd_special = PAGE_SIZE` (heap pages have no special
    /// space, §二).
    pub fn init(page: &mut [u8; PAGE_SIZE]) {
        // Zero the whole page, not just the header: a recycled buffer-pool
        // frame still holds the previous tenant's bytes, and the LP array /
        // tuple region must start clean.
        page.fill(0);
        PageHeader::init_page(page);
        if cfg!(debug_assertions) {
            debug_assert_invariants(page);
        }
    }

    /// Decode the page header.
    pub fn header(page: &[u8; PAGE_SIZE]) -> PageHeader {
        PageHeader::read_from(page)
    }

    /// Number of line pointer slots (including `Unused` ones).
    ///
    /// Infallible: derives the count from `pd_lower` arithmetic only. A
    /// corrupted header yields a garbage count but never panics; mutation and
    /// dereference paths go through [`SlottedPage::checked_header`] instead.
    pub fn slot_count(page: &[u8; PAGE_SIZE]) -> usize {
        let header = Self::header(page);
        (header.pd_lower as usize).saturating_sub(PAGE_HEADER_SIZE) / LINE_POINTER_SIZE
    }

    /// Contiguous free space in bytes (`pd_upper - pd_lower`, saturating at 0
    /// for a corrupted header).
    pub fn free_space(page: &[u8; PAGE_SIZE]) -> usize {
        let header = Self::header(page);
        header.pd_upper.saturating_sub(header.pd_lower) as usize
    }

    /// Read the line pointer at `slot`. Returns [`HeapError::InvalidSlot`]
    /// if `slot` is out of range, or [`HeapError::Corrupted`] if the header
    /// geometry is inconsistent (M2 has no page checksums, so corrupted
    /// bytes can reach this layer; they must never cause a panic).
    pub fn line_pointer(page: &[u8; PAGE_SIZE], slot: u16) -> Result<LinePointer> {
        let header = Self::checked_header(page)?;
        let slot_count = (header.pd_lower as usize - PAGE_HEADER_SIZE) / LINE_POINTER_SIZE;
        if slot as usize >= slot_count {
            return Err(HeapError::InvalidSlot(slot));
        }
        // checked_header guarantees pd_lower <= PAGE_SIZE, so this slice is
        // always in bounds.
        let off = PAGE_HEADER_SIZE + slot as usize * LINE_POINTER_SIZE;
        Ok(LinePointer::from_le_bytes(
            page[off..off + LINE_POINTER_SIZE].try_into().unwrap(),
        ))
    }

    /// Decode the header and validate the geometry the LP array depends on:
    /// `PAGE_HEADER_SIZE <= pd_lower <= pd_upper <= pd_special <= PAGE_SIZE`
    /// and `pd_lower` on a line-pointer boundary.
    fn checked_header(page: &[u8; PAGE_SIZE]) -> Result<PageHeader> {
        let header = Self::header(page);
        let (lower, upper, special) = (
            header.pd_lower as usize,
            header.pd_upper as usize,
            header.pd_special as usize,
        );
        if lower < PAGE_HEADER_SIZE
            || (lower - PAGE_HEADER_SIZE) % LINE_POINTER_SIZE != 0
            || lower > upper
            || upper > special
            || special > PAGE_SIZE
        {
            return Err(HeapError::Corrupted(format!(
                "bad page header geometry: pd_lower={lower} pd_upper={upper} pd_special={special}"
            )));
        }
        Ok(header)
    }

    /// Insert `bytes` as a new tuple, returning its slot id.
    ///
    /// Prefers recycling an [`LpFlags::Unused`] slot (LP array only grows,
    /// keeping TIDs stable); otherwise appends a new LP at `pd_lower`.
    /// Tuple bytes are placed at `pd_upper - len`.
    pub fn add_tuple(page: &mut [u8; PAGE_SIZE], bytes: &[u8]) -> Result<u16> {
        let len = bytes.len();
        let max_tuple = PAGE_SIZE - PAGE_HEADER_SIZE - LINE_POINTER_SIZE;
        if len == 0 {
            return Err(HeapError::InvalidArgument(
                "cannot insert an empty tuple".to_string(),
            ));
        }
        if len > max_tuple {
            return Err(HeapError::TupleTooLarge(len));
        }

        let header = Self::checked_header(page)?;
        let slot_count = (header.pd_lower as usize - PAGE_HEADER_SIZE) / LINE_POINTER_SIZE;

        // Find a recyclable Unused slot (first-fit). Reads the LP array
        // directly; `line_pointer()` would re-decode the header per slot.
        let mut recycled_slot = None;
        for slot in 0..slot_count {
            let off = PAGE_HEADER_SIZE + slot * LINE_POINTER_SIZE;
            let lp =
                LinePointer::from_le_bytes(page[off..off + LINE_POINTER_SIZE].try_into().unwrap());
            if lp.flags() == LpFlags::Unused {
                recycled_slot = Some(slot as u16);
                break;
            }
        }

        let lp_cost = if recycled_slot.is_some() {
            0
        } else {
            LINE_POINTER_SIZE
        };
        let free = (header.pd_upper - header.pd_lower) as usize;
        if free < len + lp_cost {
            return Err(HeapError::PageFull {
                needed: len + lp_cost,
                available: free,
            });
        }

        // Place the tuple bytes at the top of the free space.
        let new_upper = header.pd_upper as usize - len;
        page[new_upper..new_upper + len].copy_from_slice(bytes);

        let slot = match recycled_slot {
            Some(slot) => {
                Self::set_line_pointer(
                    page,
                    slot,
                    LinePointer::new(new_upper as u16, LpFlags::Normal, len as u16),
                );
                slot
            }
            None => {
                let slot = slot_count as u16;
                Self::set_line_pointer(
                    page,
                    slot,
                    LinePointer::new(new_upper as u16, LpFlags::Normal, len as u16),
                );
                let header = Self::header(page);
                Self::set_pd_lower(page, header.pd_lower + LINE_POINTER_SIZE as u16);
                slot
            }
        };
        Self::set_pd_upper(page, new_upper as u16);

        if cfg!(debug_assertions) {
            debug_assert_invariants(page);
        }
        Ok(slot)
    }

    /// Mark the tuple at `slot` as deleted (LP → [`LpFlags::Unused`]).
    ///
    /// The physical bytes are not reclaimed in-place (no compaction in this
    /// stage); the slot becomes recyclable by [`SlottedPage::add_tuple`].
    /// Returns [`HeapError::InvalidSlot`] if the slot is out of range or does
    /// not hold a live (`Normal`) tuple.
    pub fn delete_tuple(page: &mut [u8; PAGE_SIZE], slot: u16) -> Result<()> {
        let lp = Self::line_pointer(page, slot)?;
        if lp.flags() != LpFlags::Normal {
            return Err(HeapError::InvalidSlot(slot));
        }
        Self::set_line_pointer(page, slot, lp.with_flags(LpFlags::Unused));
        if cfg!(debug_assertions) {
            debug_assert_invariants(page);
        }
        Ok(())
    }

    /// Return the tuple bytes at `slot`, or `Ok(None)` if the slot is out of
    /// range or not in [`LpFlags::Normal`] state.
    ///
    /// Returns [`HeapError::Corrupted`] if the header geometry or the line
    /// pointer's offset/length are inconsistent — corrupted page bytes must
    /// never cause an out-of-bounds panic (M2 has no page checksums).
    pub fn tuple(page: &[u8; PAGE_SIZE], slot: u16) -> Result<Option<&[u8]>> {
        let lp = match Self::line_pointer(page, slot) {
            Ok(lp) => lp,
            Err(HeapError::InvalidSlot(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        if lp.flags() != LpFlags::Normal {
            return Ok(None);
        }
        let header = Self::checked_header(page)?;
        let off = lp.off() as usize;
        let end = off + lp.len() as usize;
        if off < header.pd_upper as usize || end > header.pd_special as usize {
            return Err(HeapError::Corrupted(format!(
                "slot {slot}: tuple region [{off}, {end}) outside [{}, {})",
                header.pd_upper, header.pd_special
            )));
        }
        // checked_header guarantees pd_special <= PAGE_SIZE, so this slice is
        // always in bounds.
        Ok(Some(&page[off..end]))
    }

    /// Write a line pointer into the LP array.
    fn set_line_pointer(page: &mut [u8; PAGE_SIZE], slot: u16, lp: LinePointer) {
        let off = PAGE_HEADER_SIZE + slot as usize * LINE_POINTER_SIZE;
        page[off..off + LINE_POINTER_SIZE].copy_from_slice(&lp.to_le_bytes());
    }

    /// Update `pd_lower` in the header (offset 14..16, §二).
    fn set_pd_lower(page: &mut [u8; PAGE_SIZE], pd_lower: u16) {
        page[14..16].copy_from_slice(&pd_lower.to_le_bytes());
    }

    /// Update `pd_upper` in the header (offset 16..18, §二).
    fn set_pd_upper(page: &mut [u8; PAGE_SIZE], pd_upper: u16) {
        page[16..18].copy_from_slice(&pd_upper.to_le_bytes());
    }
}

/// Assert the slotted-page invariants listed in the module docs:
/// `pd_lower <= pd_upper`, LP regions within `[pd_upper, pd_special)`, and
/// no overlapping tuple regions.
///
/// Intended for tests and `debug_assert!` use; compiled in always so
/// integration tests and proptests can call it.
pub fn debug_assert_invariants(page: &[u8; PAGE_SIZE]) {
    let header = SlottedPage::header(page);
    assert!(header.pd_lower as usize >= PAGE_HEADER_SIZE);
    assert!(header.pd_lower <= header.pd_upper);
    assert!(header.pd_upper <= header.pd_special);
    assert!(header.pd_special as usize <= PAGE_SIZE);
    assert_eq!(
        (header.pd_lower as usize - PAGE_HEADER_SIZE) % LINE_POINTER_SIZE,
        0
    );

    let mut regions: Vec<(usize, usize)> = Vec::new();
    for slot in 0..SlottedPage::slot_count(page) {
        let lp = SlottedPage::line_pointer(page, slot as u16).unwrap();
        if lp.flags() == LpFlags::Unused {
            continue;
        }
        let off = lp.off() as usize;
        let end = off + lp.len() as usize;
        assert!(
            off >= header.pd_upper as usize,
            "slot {slot} below pd_upper"
        );
        assert!(
            end <= header.pd_special as usize,
            "slot {slot} past pd_special"
        );
        regions.push((off, end));
    }
    regions.sort_unstable();
    for pair in regions.windows(2) {
        assert!(
            pair[0].1 <= pair[1].0,
            "overlapping tuple regions: {:?} vs {:?}",
            pair[0],
            pair[1]
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_page() -> [u8; PAGE_SIZE] {
        let mut page = [0u8; PAGE_SIZE];
        SlottedPage::init(&mut page);
        page
    }

    #[test]
    fn init_sets_header_fields() {
        let page = fresh_page();
        let header = SlottedPage::header(&page);
        assert_eq!(header.pd_lower, PAGE_HEADER_SIZE as u16);
        assert_eq!(header.pd_upper, PAGE_SIZE as u16);
        assert_eq!(header.pd_special, PAGE_SIZE as u16);
        assert_eq!(SlottedPage::slot_count(&page), 0);
        assert_eq!(SlottedPage::free_space(&page), PAGE_SIZE - PAGE_HEADER_SIZE);
    }

    #[test]
    fn add_and_read_back() {
        let mut page = fresh_page();
        let slot = SlottedPage::add_tuple(&mut page, b"hello heap").unwrap();
        assert_eq!(slot, 0);
        assert_eq!(
            SlottedPage::tuple(&page, slot).unwrap(),
            Some(&b"hello heap"[..])
        );
        assert_eq!(SlottedPage::slot_count(&page), 1);
    }

    #[test]
    fn delete_recycles_slot() {
        let mut page = fresh_page();
        let s0 = SlottedPage::add_tuple(&mut page, b"aaaa").unwrap();
        let _s1 = SlottedPage::add_tuple(&mut page, b"bbbb").unwrap();
        SlottedPage::delete_tuple(&mut page, s0).unwrap();
        assert_eq!(SlottedPage::tuple(&page, s0).unwrap(), None);
        // The recycled slot is reused for the next insert.
        let s2 = SlottedPage::add_tuple(&mut page, b"cccc").unwrap();
        assert_eq!(s2, s0);
        assert_eq!(SlottedPage::tuple(&page, s2).unwrap(), Some(&b"cccc"[..]));
        assert_eq!(SlottedPage::slot_count(&page), 2);
    }

    #[test]
    fn page_full_is_reported() {
        let mut page = fresh_page();
        let big = vec![0xAB; PAGE_SIZE];
        assert!(matches!(
            SlottedPage::add_tuple(&mut page, &big),
            Err(HeapError::TupleTooLarge(_))
        ));
        // Fill the page with maximal tuples.
        let chunk = vec![0xCD; 1000];
        while SlottedPage::free_space(&page) >= 1000 + LINE_POINTER_SIZE {
            SlottedPage::add_tuple(&mut page, &chunk).unwrap();
        }
        let err = SlottedPage::add_tuple(&mut page, &chunk).unwrap_err();
        assert!(matches!(err, HeapError::PageFull { .. }));
    }

    #[test]
    fn invalid_slots_rejected() {
        let mut page = fresh_page();
        assert!(matches!(
            SlottedPage::delete_tuple(&mut page, 0),
            Err(HeapError::InvalidSlot(0))
        ));
        assert_eq!(SlottedPage::tuple(&page, 7).unwrap(), None);
        assert!(matches!(
            SlottedPage::line_pointer(&page, 0),
            Err(HeapError::InvalidSlot(0))
        ));
    }

    #[test]
    fn empty_tuple_rejected_as_invalid_argument() {
        let mut page = fresh_page();
        assert!(matches!(
            SlottedPage::add_tuple(&mut page, &[]),
            Err(HeapError::InvalidArgument(_))
        ));
    }
}
