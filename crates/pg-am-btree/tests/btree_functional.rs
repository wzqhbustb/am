//! Stage M single-threaded functional tests: ascending/descending/random
//! bulk inserts with multi-level splits, point lookups, range scans,
//! duplicate keys, physical deletes, structural validation, and reopen.

use std::sync::Arc;

use pg_am_btree::key::{decode_i32, encode_i32};
use pg_am_btree::{BTreeAM, BTreeError, BTreeIndex};

use pg_am_heap::tuple::ColumnType;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid};

use tempfile::TempDir;

const REL_OID: Oid = Oid(16_385);
/// Number of keys in the bulk tests: enough to force dozens of leaf splits
/// and a root split (~18 B per Int4 entry, ~450 entries per page).
const BULK_KEYS: i32 = 12_000;

fn tid(i: u64) -> Tid {
    Tid {
        page_id: PageId(42_000 + i / 60_000),
        slot_id: (i % 60_000) as u16,
    }
}

fn key(i: i32) -> Vec<u8> {
    encode_i32(i).to_vec()
}

fn setup() -> (TempDir, StorageEngine, BTreeIndex) {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let index = am.create_index(REL_OID, ColumnType::Int4).unwrap();
    (tmp, engine, index)
}

/// Every key in `0..n` must point-lookup to its own TID.
fn assert_all_present(index: &BTreeIndex, n: i32) {
    for i in 0..n {
        assert_eq!(
            index.lookup(&key(i)).unwrap(),
            Some(tid(i as u64)),
            "key {i} must be found"
        );
    }
    // And keys outside the range must miss.
    assert_eq!(index.lookup(&key(-1)).unwrap(), None);
    assert_eq!(index.lookup(&key(n)).unwrap(), None);
}

#[test]
fn ascending_bulk_insert_lookup_scan_validate() {
    let (_tmp, _engine, mut index) = setup();
    for i in 0..BULK_KEYS {
        index.insert(&key(i), tid(i as u64)).unwrap();
    }
    assert!(
        index.tree_level() >= 1,
        "12k keys must have split the root leaf"
    );
    assert_all_present(&index, BULK_KEYS);

    // Range scan boundaries: [100, 105) yields exactly 100..104 in order.
    let rows = index.range_scan(Some(&key(100)), Some(&key(105))).unwrap();
    let got: Vec<i32> = rows
        .iter()
        .map(|(k, _)| decode_i32(k.clone().try_into().unwrap()))
        .collect();
    assert_eq!(got, vec![100, 101, 102, 103, 104]);

    // Open start: everything below 3.
    let rows = index.range_scan(None, Some(&key(3))).unwrap();
    assert_eq!(rows.len(), 3);

    // Open end: the last two keys.
    let rows = index.range_scan(Some(&key(BULK_KEYS - 2)), None).unwrap();
    assert_eq!(rows.len(), 2);

    // Full scan returns everything, in order, with no duplicates.
    let rows = index.range_scan(None, None).unwrap();
    assert_eq!(rows.len(), BULK_KEYS as usize);
    for (i, (k, t)) in rows.iter().enumerate() {
        assert_eq!(k.as_slice(), key(i as i32).as_slice());
        assert_eq!(*t, tid(i as u64));
    }

    index.validate().unwrap();
}

#[test]
fn descending_bulk_insert_lookup_validate() {
    let (_tmp, _engine, mut index) = setup();
    for i in (0..BULK_KEYS).rev() {
        index.insert(&key(i), tid(i as u64)).unwrap();
    }
    assert_all_present(&index, BULK_KEYS);
    index.validate().unwrap();
}

#[test]
fn random_bulk_insert_lookup_validate() {
    let (_tmp, _engine, mut index) = setup();
    // Deterministic shuffle via an LCG over the full range.
    let mut perm: Vec<i32> = (0..BULK_KEYS).collect();
    let mut state: u64 = 0x1234_5678_9ABC_DEF0;
    for i in (1..perm.len()).rev() {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        let j = (state >> 33) as usize % (i + 1);
        perm.swap(i, j);
    }
    for i in perm {
        index.insert(&key(i), tid(i as u64)).unwrap();
    }
    assert_all_present(&index, BULK_KEYS);
    index.validate().unwrap();
}

#[test]
fn multi_level_tree_with_wide_keys() {
    // 32-byte Text keys shrink the fan-out so ~40k keys produce a 3-level
    // tree (root -> internal -> leaf).
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let mut index = am.create_index(REL_OID, ColumnType::Text).unwrap();

    let wide_key = |i: u32| format!("key-{i:028}").into_bytes();
    let n = 40_000u32;
    for i in 0..n {
        index.insert(&wide_key(i), tid(i as u64)).unwrap();
    }
    assert!(
        index.tree_level() >= 2,
        "40k wide keys must produce a multi-level tree, got level {}",
        index.tree_level()
    );
    index.validate().unwrap();

    // Spot-check lookups across the range.
    for i in [0, 1, n / 3, n / 2, n - 2, n - 1] {
        assert_eq!(index.lookup(&wide_key(i)).unwrap(), Some(tid(i as u64)));
    }
    let rows = index
        .range_scan(Some(&wide_key(1000)), Some(&wide_key(1005)))
        .unwrap();
    assert_eq!(rows.len(), 5);
}

#[test]
fn duplicate_keys_allowed_with_distinct_tids() {
    let (_tmp, _engine, mut index) = setup();
    let k = key(7);
    for i in 0..500u64 {
        index.insert(&k, tid(i)).unwrap();
    }
    // The exact (key, tid) pair is rejected.
    assert!(matches!(
        index.insert(&k, tid(0)),
        Err(BTreeError::DuplicateKey)
    ));
    // Lookup returns the first entry with the key.
    assert_eq!(index.lookup(&k).unwrap(), Some(tid(0)));
    // Range scan returns every duplicate in (key, tid) order.
    let rows = index.range_scan(Some(&k), None).unwrap();
    assert_eq!(rows.len(), 500);
    for (i, (_, t)) in rows.iter().enumerate() {
        assert_eq!(*t, tid(i as u64));
    }
    index.validate().unwrap();
}

#[test]
fn delete_removes_entries_physically() {
    let (_tmp, _engine, mut index) = setup();
    let n = 5_000i32;
    for i in 0..n {
        index.insert(&key(i), tid(i as u64)).unwrap();
    }
    // Delete every third key.
    let mut i = 0;
    while i < n {
        index.delete(&key(i), tid(i as u64)).unwrap();
        i += 3;
    }
    for i in 0..n {
        let expect = if i % 3 == 0 {
            None
        } else {
            Some(tid(i as u64))
        };
        assert_eq!(index.lookup(&key(i)).unwrap(), expect, "key {i}");
    }
    // Deleting a missing entry is EntryNotFound.
    assert!(matches!(
        index.delete(&key(0), tid(0)),
        Err(BTreeError::EntryNotFound)
    ));
    assert!(matches!(
        index.delete(&key(1), tid(999_999)),
        Err(BTreeError::EntryNotFound)
    ));
    index.validate().unwrap();

    // The freed LP space is reusable: reinsert the deleted keys.
    let mut i = 0;
    while i < n {
        index.insert(&key(i), tid(i as u64)).unwrap();
        i += 3;
    }
    assert_all_present(&index, n);
    index.validate().unwrap();
}

#[test]
fn reopen_recovers_root_from_meta_page() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let meta_page;
    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let am = BTreeAM::new(
            Arc::clone(engine.buffer_pool()),
            Arc::clone(engine.wal_writer()),
        );
        let mut index = am.create_index(REL_OID, ColumnType::Int4).unwrap();
        meta_page = index.meta_page();
        for i in 0..3_000i32 {
            index.insert(&key(i), tid(i as u64)).unwrap();
        }
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine); // crash: no checkpoint, no shutdown
    }
    let engine = StorageEngine::open_with_redo_handlers(
        tmp.path(),
        &config,
        pg_am_btree::btree_redo_handlers(),
    )
    .unwrap();
    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let index = am.open_index(REL_OID, meta_page, ColumnType::Int4).unwrap();
    assert!(index.tree_level() >= 1);
    assert_all_present(&index, 3_000);
    index.validate().unwrap();
}

/// P1-2 regression: a page whose split lost its Commit (SPLIT_INCOMPLETE
/// with an unlinked right twin) must refuse a SECOND split — otherwise the
/// new split re-points `left.next` and orphans the first twin forever.
#[test]
fn second_split_of_incomplete_page_is_rejected() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let meta_page;
    let left;
    let n;
    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let am = BTreeAM::new(
            Arc::clone(engine.buffer_pool()),
            Arc::clone(engine.wal_writer()),
        );
        let mut index = am.create_index(REL_OID, ColumnType::Int4).unwrap();
        // Fill the root leaf, then run ONLY split_prepare (the Commit is
        // "lost" — we simply never run it).
        const ENTRY_BYTES: usize = 4 + 10 + 4;
        let mut i = 0i32;
        while index.page_free_space(index.root_page()).unwrap() >= ENTRY_BYTES {
            index.insert(&key(i), tid(i as u64)).unwrap();
            i += 1;
        }
        n = i;
        left = index.root_page();
        let st = index.split_prepare(left).unwrap();
        meta_page = index.meta_page();
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine); // crash with only Prepare durable
        let _ = st;
    }

    let engine = StorageEngine::open_with_redo_handlers(
        tmp.path(),
        &config,
        pg_am_btree::btree_redo_handlers(),
    )
    .unwrap();
    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let mut index = am.open_index(REL_OID, meta_page, ColumnType::Int4).unwrap();

    // The left page is full and SPLIT_INCOMPLETE: any insert that targets
    // it must surface the guard (not silently split it a second time).
    let err = index.insert(&key(1_000_000), tid(9_999_999)).unwrap_err();
    assert!(
        matches!(err, BTreeError::Unsupported(_)),
        "expected Unsupported from the SPLIT_INCOMPLETE guard, got {err:?}"
    );
    // Directly driving the step API hits the same guard.
    assert!(matches!(
        index.split_prepare(left),
        Err(BTreeError::Unsupported(_))
    ));
    // Existing keys are still all readable through the incomplete split.
    for i in [0, n / 2, n - 1] {
        assert_eq!(index.lookup(&key(i)).unwrap(), Some(tid(i as u64)));
    }
}

/// P2-1 regression: a handle whose cached root was promoted by ANOTHER
/// handle must not fork the tree — the root-split branch revalidates
/// against the meta page and fails loudly instead of creating a second
/// root and overwriting the meta record.
#[test]
fn stale_root_handle_cannot_fork_the_tree() {
    let (_tmp, _engine, mut a) = setup();
    let am = BTreeAM::new(
        Arc::clone(_engine.buffer_pool()),
        Arc::clone(_engine.wal_writer()),
    );
    let meta_page = a.meta_page();
    let mut b = am.open_index(REL_OID, meta_page, ColumnType::Int4).unwrap();

    // A inserts enough to promote the root (tree_level 0 -> 1).
    for i in 0..3_000i32 {
        a.insert(&key(i), tid(i as u64)).unwrap();
    }
    assert!(a.tree_level() >= 1, "A must have promoted the root");
    let current_root = a.root_page();
    assert_ne!(b.root_page(), current_root, "B still caches the old root");

    // B keeps inserting into the OLD root leaf (now a regular leftmost
    // leaf; duplicate keys with fresh TIDs land on it) until it splits.
    // The split's Commit must detect the staleness via the meta page.
    let mut err = None;
    for i in 0..10_000u64 {
        match b.insert(&key(i as i32 % 500), tid(5_000_000 + i)) {
            Ok(()) => {}
            Err(e) => {
                err = Some(e);
                break;
            }
        }
    }
    let err = err.expect("B's split of the stale root must fail");
    assert!(
        matches!(err, BTreeError::Unsupported(_)),
        "expected stale-root Unsupported, got {err:?}"
    );

    // A's tree is untouched: meta still points at A's root, everything
    // validates, all of A's keys are present.
    assert_eq!(a.root_page(), current_root);
    a.validate().unwrap();
    assert_all_present(&a, 3_000);
}
