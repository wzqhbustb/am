//! Integration tests for the slotted page (tech-selection §二).
//!
//! Acceptance command: `cargo test -p pg-am-heap --test slotted_page`.

use pg_am_heap::slotted_page::{debug_assert_invariants, SlottedPage};
use pg_am_heap::{HeapError, LpFlags};
use pg_storage::page::PAGE_HEADER_SIZE;
use pg_storage::types::PAGE_SIZE;

use proptest::prelude::*;

fn fresh_page() -> [u8; PAGE_SIZE] {
    let mut page = [0u8; PAGE_SIZE];
    SlottedPage::init(&mut page);
    page
}

/// Stage G acceptance: after adds and deletes, `pd_lower <= pd_upper`,
/// line pointers never overlap, and `Unused` slots are recycled.
#[test]
fn test_slotted_page_add_delete_invariant() {
    let mut page = fresh_page();

    // Insert a mix of tuple sizes.
    let mut slots = Vec::new();
    for i in 0..20u16 {
        let bytes = vec![i as u8; 32 + i as usize * 7];
        slots.push(SlottedPage::add_tuple(&mut page, &bytes).unwrap());
    }
    debug_assert_invariants(&page);
    let lower_after_inserts = SlottedPage::header(&page).pd_lower;
    assert_eq!(SlottedPage::slot_count(&page), 20);

    // Delete every other tuple; slot count must not shrink (LP array only
    // grows — TID stability, §二).
    for &slot in slots.iter().step_by(2) {
        SlottedPage::delete_tuple(&mut page, slot).unwrap();
        debug_assert_invariants(&page);
    }
    assert_eq!(SlottedPage::slot_count(&page), 20);
    assert_eq!(SlottedPage::header(&page).pd_lower, lower_after_inserts);

    // Deleted slots read back as None; live slots keep their bytes.
    for (i, &slot) in slots.iter().enumerate() {
        if i % 2 == 0 {
            assert_eq!(SlottedPage::tuple(&page, slot).unwrap(), None);
        } else {
            let expected = vec![i as u8; 32 + i * 7];
            assert_eq!(
                SlottedPage::tuple(&page, slot).unwrap(),
                Some(&expected[..])
            );
        }
    }

    // Re-inserts recycle the Unused slots (first-fit order) without growing
    // the LP array.
    for &slot in slots.iter().step_by(2) {
        let new_slot = SlottedPage::add_tuple(&mut page, b"recycled").unwrap();
        debug_assert_invariants(&page);
        assert_eq!(new_slot, slot, "expected Unused slot {slot} to be recycled");
    }
    assert_eq!(SlottedPage::slot_count(&page), 20);
    assert_eq!(SlottedPage::header(&page).pd_lower, lower_after_inserts);

    // Header invariant: pd_lower <= pd_upper at all times (checked above by
    // debug_assert_invariants), and free space accounting is consistent.
    let header = SlottedPage::header(&page);
    assert!(PAGE_HEADER_SIZE <= header.pd_lower as usize);
    assert!(header.pd_lower <= header.pd_upper);
    assert_eq!(
        SlottedPage::free_space(&page),
        (header.pd_upper - header.pd_lower) as usize
    );
}

/// Deleting a live slot marks its LP `Unused` while keeping offset/length.
#[test]
fn delete_marks_lp_unused() {
    let mut page = fresh_page();
    let slot = SlottedPage::add_tuple(&mut page, b"data").unwrap();
    let before = SlottedPage::line_pointer(&page, slot).unwrap();
    assert_eq!(before.flags(), LpFlags::Normal);

    SlottedPage::delete_tuple(&mut page, slot).unwrap();
    let after = SlottedPage::line_pointer(&page, slot).unwrap();
    assert_eq!(after.flags(), LpFlags::Unused);
    assert_eq!(after.off(), before.off());
    assert_eq!(after.len(), before.len());

    // Double delete is an error, not silent corruption.
    assert!(matches!(
        SlottedPage::delete_tuple(&mut page, slot),
        Err(HeapError::InvalidSlot(_))
    ));
}

/// Insert until full: every intermediate state must satisfy the invariants.
#[test]
fn fill_page_to_full_keeps_invariants() {
    let mut page = fresh_page();
    let bytes = vec![0x5Au8; 100];
    let mut n = 0usize;
    loop {
        match SlottedPage::add_tuple(&mut page, &bytes) {
            Ok(_) => {
                n += 1;
                debug_assert_invariants(&page);
            }
            Err(HeapError::PageFull { .. }) => break,
            Err(e) => panic!("unexpected error: {e}"),
        }
    }
    // 8160 free bytes, each tuple costs 100 + 4 LP bytes.
    assert_eq!(n, (PAGE_SIZE - PAGE_HEADER_SIZE) / 104);
    assert!(SlottedPage::free_space(&page) < 104);
}

#[derive(Debug, Clone, Copy)]
enum Op {
    Insert(u16),
    Delete(u16),
}

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        // Insert a tuple of 8..600 bytes.
        (8u16..600u16).prop_map(Op::Insert),
        // Delete slot index (clamped to the live set at apply time).
        any::<u16>().prop_map(Op::Delete),
    ]
}

// Stage G acceptance: random insert/delete sequences preserve the slotted
// page invariants (`pd_lower <= pd_upper`, no overlapping LPs).
//
// The tech-selection asks for 1M operations. The defaults below give
// 5000 cases x ~200 ops average (vec bound 1..400) ~= 1M ops per run.
// `PROPTEST_CASES` can override the scale in either direction.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(
        std::env::var("PROPTEST_CASES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5000)
    ))]

    #[test]
    fn proptest_random_ops_preserve_invariants(ops in prop::collection::vec(op_strategy(), 1..400)) {
        let mut page = fresh_page();
        // Model: slot -> live bytes (slot ids are stable, Unused recycled).
        let mut model: std::collections::BTreeMap<u16, Vec<u8>> = Default::default();

        for op in ops {
            match op {
                Op::Insert(len_seed) => {
                    let len = 8 + (len_seed as usize % 593);
                    let bytes = vec![(len_seed & 0xFF) as u8; len];
                    match SlottedPage::add_tuple(&mut page, &bytes) {
                        Ok(slot) => {
                            model.insert(slot, bytes);
                        }
                        Err(HeapError::PageFull { .. }) => {}
                        Err(e) => panic!("unexpected add_tuple error: {e}"),
                    }
                }
                Op::Delete(seed) => {
                    if !model.is_empty() {
                        let idx = seed as usize % model.len();
                        let slot = *model.keys().nth(idx).unwrap();
                        SlottedPage::delete_tuple(&mut page, slot).unwrap();
                        model.remove(&slot);
                    }
                }
            }
            debug_assert_invariants(&page);
        }

        // Final state: model and page agree on every slot.
        let live: std::collections::BTreeMap<u16, Vec<u8>> = (0..SlottedPage::slot_count(&page) as u16)
            .filter_map(|s| SlottedPage::tuple(&page, s).unwrap().map(|t| (s, t.to_vec())))
            .collect();
        prop_assert_eq!(live, model);
    }
}

// ---------------------------------------------------------------------------
// Corrupted-input robustness (M2 has no page checksums: bit rot reaches the
// AM undetected, so corrupted page bytes must produce errors, never panics
// or silent scribbling).
// ---------------------------------------------------------------------------

/// A corrupted `pd_lower` makes the LP-array geometry inconsistent;
/// `line_pointer` must return `Corrupted` instead of slicing out of bounds.
#[test]
fn corrupted_pd_lower_returns_error_not_panic() {
    let mut page = fresh_page();
    SlottedPage::add_tuple(&mut page, b"abc").unwrap();
    page[14..16].copy_from_slice(&0xFFFFu16.to_le_bytes()); // pd_lower = 0xFFFF
    assert!(matches!(
        SlottedPage::line_pointer(&page, 5000),
        Err(HeapError::Corrupted(_))
    ));
    // Even a small slot index is rejected: the geometry itself is invalid.
    assert!(matches!(
        SlottedPage::line_pointer(&page, 0),
        Err(HeapError::Corrupted(_))
    ));
    assert!(matches!(
        SlottedPage::tuple(&page, 0),
        Err(HeapError::Corrupted(_))
    ));
}

/// A corrupted line pointer (offset/length beyond the tuple region) must
/// yield `Corrupted`, not an out-of-bounds slice.
#[test]
fn corrupted_lp_region_returns_error_not_panic() {
    let mut page = fresh_page();
    let slot = SlottedPage::add_tuple(&mut page, b"abc").unwrap();
    assert_eq!(slot, 0);
    // lp_off = 0x7FFF, lp_flags = Normal, lp_len = 0x7FFF.
    let raw: u32 = 0x7FFF | (1 << 15) | (0x7FFF << 17);
    page[32..36].copy_from_slice(&raw.to_le_bytes());
    assert!(matches!(
        SlottedPage::tuple(&page, slot),
        Err(HeapError::Corrupted(_))
    ));
}

/// `pd_upper < pd_lower` (inverted free-space geometry): `free_space`
/// saturates to 0 and `add_tuple` returns `Corrupted` instead of wrapping
/// the u16 subtraction and scribbling over the LP array in release builds.
#[test]
fn inverted_pd_upper_lower_rejected() {
    let mut page = fresh_page();
    SlottedPage::add_tuple(&mut page, b"abc").unwrap();
    page[14..16].copy_from_slice(&100u16.to_le_bytes()); // pd_lower = 100
    page[16..18].copy_from_slice(&40u16.to_le_bytes()); // pd_upper = 40
    assert_eq!(SlottedPage::free_space(&page), 0);
    assert!(matches!(
        SlottedPage::add_tuple(&mut page, b"x"),
        Err(HeapError::Corrupted(_))
    ));
}
