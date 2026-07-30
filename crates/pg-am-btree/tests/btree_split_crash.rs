//! Stage M acceptance crash tests (coding-plan Stage M):
//! `test_btree_split_crash_after_prepare` / `..._after_copy` /
//! `..._after_commit`.
//!
//! Each test drives a root-leaf split one WAL step at a time through the
//! internal step API (`split_prepare` / `split_copy` / `split_commit`),
//! abandons the engine mid-protocol (`mem::forget`: no checkpoint, no
//! shutdown — a kill -9), then reopens with the B+Tree redo handlers and
//! asserts:
//!
//! - the tree is structurally sound for the reached step: the `btpo_next`
//!   chain walks end to end, and (after Commit) the strict
//!   [`BTreeIndex::validate`] passes;
//! - no key is lost or duplicated: the full leaf-chain scan equals the
//!   inserted set and every point lookup hits (Blink right hops reach keys
//!   on a right sibling whose downlink was never committed);
//! - replay is idempotent: crashing *again* after recovery and replaying
//!   the same records a second time converges to the same state.

use std::path::Path;
use std::sync::Arc;

use pg_am_btree::page::{BtreePage, BTREE_FLAG_SPLIT_INCOMPLETE};
use pg_am_btree::{btree_redo_handlers, BTreeAM, BTreeIndex};

use pg_am_heap::slotted_page::SlottedPage;
use pg_am_heap::tuple::ColumnType;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid, PAGE_SIZE};

use tempfile::TempDir;

const REL_OID: Oid = Oid(16_388);

fn tid(i: u64) -> Tid {
    Tid {
        page_id: PageId(42_000 + i),
        slot_id: i as u16,
    }
}

fn key(i: i32) -> Vec<u8> {
    pg_am_btree::encode_i32(i).to_vec()
}

fn decode(rows: &[(Vec<u8>, Tid)]) -> Vec<i32> {
    rows.iter()
        .map(|(k, _)| pg_am_btree::decode_i32(k.clone().try_into().unwrap()))
        .collect()
}

/// Create an index and fill its root leaf until the next entry would no
/// longer fit (the next insert would split). Returns the key count.
fn create_and_fill(dir: &Path, config: &StorageConfig) -> (StorageEngine, BTreeIndex, i32) {
    let engine = StorageEngine::open(dir, config).unwrap();
    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let mut index = am.create_index(REL_OID, ColumnType::Int4).unwrap();
    // i32 key (4B) + tid trailer (10B) + line pointer (4B).
    const ENTRY_BYTES: usize = 4 + 10 + 4;
    let mut n = 0i32;
    while index.page_free_space(index.root_page()).unwrap() >= ENTRY_BYTES {
        index.insert(&key(n), tid(n as u64)).unwrap();
        n += 1;
    }
    assert!(n > 100, "a page must hold a meaningful number of entries");
    (engine, index, n)
}

/// Reopen after the simulated crash with the B+Tree redo handlers
/// registered, and rebuild the index handle from the meta page.
fn recover(dir: &Path, config: &StorageConfig, meta_page: PageId) -> (StorageEngine, BTreeIndex) {
    let engine =
        StorageEngine::open_with_redo_handlers(dir, config, btree_redo_handlers()).unwrap();
    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let index = am.open_index(REL_OID, meta_page, ColumnType::Int4).unwrap();
    (engine, index)
}

/// The no-key-lost-no-key-duplicated assertion: the full leaf-chain scan
/// equals `0..n` in order and every point lookup hits.
fn assert_all_keys(index: &BTreeIndex, n: i32) {
    let rows = index.range_scan(None, None).unwrap();
    let got = decode(&rows);
    let want: Vec<i32> = (0..n).collect();
    assert_eq!(got, want, "leaf chain must hold every key exactly once");
    for i in 0..n {
        assert_eq!(
            index.lookup(&key(i)).unwrap(),
            Some(tid(i as u64)),
            "key {i} must be reachable by point lookup"
        );
    }
}

/// Walk the `btpo_next` chain from `start`, returning the page ids.
fn chain_from(engine: &StorageEngine, start: PageId) -> Vec<PageId> {
    let mut out = vec![start];
    loop {
        let cur = *out.last().unwrap();
        let guard = engine.buffer_pool().pin(cur).unwrap();
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
        let next = BtreePage::next(page).unwrap();
        drop(guard);
        if next == PageId::INVALID {
            return out;
        }
        out.push(next);
        assert!(out.len() < 1000, "sibling chain cycle");
    }
}

/// Read `btpo_flags` / slot count of a page.
fn page_state(engine: &StorageEngine, page_id: PageId) -> (u8, usize) {
    let guard = engine.buffer_pool().pin(page_id).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    (
        BtreePage::flags(page).unwrap(),
        SlottedPage::slot_count(page),
    )
}

/// Crash with only `BTreeSplitPrepare` durable: the left page is marked
/// SPLIT_INCOMPLETE and linked to an initialized but empty right page.
/// Recovery must rebuild exactly that state; every key still lives on the
/// left page (Copy never ran), so nothing is lost and the chain walks.
#[test]
fn test_btree_split_crash_after_prepare() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (meta_page, n, left, right) = {
        let (engine, index, n) = create_and_fill(tmp.path(), &config);
        let left = index.root_page();
        let st = index.split_prepare(left).unwrap();
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine); // kill -9: Prepare durable, nothing else
        (index.meta_page(), n, st.left, st.right)
    };

    // Recovery 1: replay the Prepare record.
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_all_keys(&index, n);
    assert_eq!(
        chain_from(&engine, index.root_page()),
        vec![left, right],
        "the sibling chain must walk left -> right end to end"
    );
    let (left_flags, left_slots) = page_state(&engine, left);
    assert_ne!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
    assert_eq!(
        left_slots, n as usize,
        "Copy never ran: left keeps all keys"
    );
    let (_, right_slots) = page_state(&engine, right);
    assert_eq!(right_slots, 0, "Copy never ran: right stays empty");
    std::mem::forget(engine); // crash again, mid/after recovery

    // Recovery 2: replaying the same records a second time must converge.
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_all_keys(&index, n);
    assert_eq!(chain_from(&engine, index.root_page()), vec![left, right]);
    let (left_flags, _) = page_state(&engine, left);
    assert_ne!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
    drop(engine);
}

/// Crash with `BTreeSplitPrepare` + `BTreeSplitCopy` durable: the left page
/// is truncated, the right page holds the upper half, but no downlink was
/// ever committed (and, being a root split, the new root was never
/// created). Recovery must rebuild that state; Blink right hops keep every
/// key reachable.
#[test]
fn test_btree_split_crash_after_copy() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (meta_page, n, left, right, copy_start_slot) = {
        let (engine, index, n) = create_and_fill(tmp.path(), &config);
        let left = index.root_page();
        let st = index.split_prepare(left).unwrap();
        index.split_copy(&st).unwrap();
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine); // kill -9: Prepare + Copy durable
        (index.meta_page(), n, st.left, st.right, st.copy_start_slot)
    };

    // Recovery 1.
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_all_keys(&index, n);
    assert_eq!(chain_from(&engine, index.root_page()), vec![left, right]);
    let (left_flags, left_slots) = page_state(&engine, left);
    assert_ne!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
    assert_eq!(left_slots, copy_start_slot as usize);
    let (_, right_slots) = page_state(&engine, right);
    assert_eq!(right_slots, n as usize - copy_start_slot as usize);
    // The two halves partition the key space.
    let left_rows = {
        let g = engine.buffer_pool().pin(left).unwrap();
        let p: &[u8; PAGE_SIZE] = g.page().try_into().unwrap();
        let mut v = Vec::new();
        for s in 0..SlottedPage::slot_count(p) as u16 {
            let (k, _) =
                pg_am_btree::page::decode_leaf_entry(SlottedPage::tuple(p, s).unwrap().unwrap())
                    .unwrap();
            v.push(pg_am_btree::decode_i32(k.try_into().unwrap()));
        }
        v
    };
    assert_eq!(
        *left_rows.last().unwrap(),
        copy_start_slot as i32 - 1,
        "left keeps the lower half"
    );
    std::mem::forget(engine); // crash again

    // Recovery 2: the Copy record's `left_page_pre_lsn` anchor makes the
    // second replay converge to the same state.
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_all_keys(&index, n);
    assert_eq!(chain_from(&engine, index.root_page()), vec![left, right]);
    let (_, left_slots) = page_state(&engine, left);
    let (_, right_slots) = page_state(&engine, right);
    assert_eq!(left_slots, copy_start_slot as usize);
    assert_eq!(right_slots, n as usize - copy_start_slot as usize);
    drop(engine);
}

/// Crash with the full split (Prepare + Copy + Commit) durable: recovery
/// must restore a fully valid two-level tree — new root from the meta page,
/// downlinks in place, no SPLIT_INCOMPLETE left behind.
#[test]
fn test_btree_split_crash_after_commit() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (meta_page, n, left, right) = {
        let (engine, mut index, n) = create_and_fill(tmp.path(), &config);
        let left = index.root_page();
        let st = index.split_prepare(left).unwrap();
        index.split_copy(&st).unwrap();
        // The split page is the root: an empty path drives the new-root
        // branch of Commit.
        index.split_commit(&st, &mut Vec::new()).unwrap();
        assert_eq!(index.tree_level(), 1);
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine); // kill -9: the whole split is durable
        (index.meta_page(), n, st.left, st.right)
    };

    // Recovery 1.
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_eq!(
        index.tree_level(),
        1,
        "meta page must point at the new root"
    );
    assert_ne!(index.root_page(), left);
    assert_all_keys(&index, n);
    index
        .validate()
        .unwrap_or_else(|e| panic!("recovered tree must pass strict validation: {e}"));
    assert_eq!(chain_from(&engine, left), vec![left, right]);
    let (left_flags, _) = page_state(&engine, left);
    assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
    // The root holds exactly two downlinks.
    let (_, root_slots) = page_state(&engine, index.root_page());
    assert_eq!(root_slots, 2);
    std::mem::forget(engine); // crash again

    // Recovery 2: idempotent re-replay of Prepare/Copy/Commit.
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_eq!(index.tree_level(), 1);
    assert_all_keys(&index, n);
    index.validate().unwrap();
    let (_, root_slots) = page_state(&engine, index.root_page());
    assert_eq!(root_slots, 2, "Commit redo must not duplicate the downlink");
    drop(engine);
}
