//! Stage S acceptance tests for ARIES undo of incomplete B+Tree splits.
//!
//! A split is a three-record protocol (Prepare / Copy / Commit). A crash
//! between Prepare and Commit leaves the left page flagged
//! `SPLIT_INCOMPLETE` with its downlink missing. Redo alone cannot fix that:
//! the records that would finish the split were never written. Recovery
//! therefore runs an undo pass that finishes the split and logs a
//! `BTreeSplitCLR` compensation record so the repair itself is crash-safe.
//!
//! These tests assert:
//!
//! - undo finishes the split: `SPLIT_INCOMPLETE` is cleared, the downlink
//!   exists, and the strict [`BTreeIndex::validate`] passes;
//! - the CLR is idempotent: crashing again and replaying it converges on the
//!   same pages, in particular the moved entries are never copied twice;
//! - both shapes are covered — a root split (new root + meta page update) and
//!   a non-root split (downlink inserted into an existing parent).

use std::path::Path;
use std::sync::Arc;

use pg_am_btree::page::{BtreePage, BTREE_FLAG_SPLIT_INCOMPLETE};
use pg_am_btree::{btree_redo_handlers, BTreeAM, BTreeIndex, BTreeUndoHandler};

use pg_am_heap::slotted_page::SlottedPage;
use pg_am_heap::tuple::ColumnType;
use pg_am_heap::HeapUndoHandler;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Lsn, Oid, PageId, Tid, PAGE_SIZE};

use tempfile::TempDir;

const REL_OID: Oid = Oid(16_401);

fn tid(i: u64) -> Tid {
    Tid {
        page_id: PageId(51_000 + i),
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
/// longer fit (so the next insert would split). Returns the key count.
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

fn recover(dir: &Path, config: &StorageConfig, meta_page: PageId) -> (StorageEngine, BTreeIndex) {
    let engine = StorageEngine::open_with_redo_handlers(
        dir,
        config,
        btree_redo_handlers(),
        vec![Box::new(HeapUndoHandler), Box::new(BTreeUndoHandler)],
    )
    .unwrap();
    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let index = am.open_index(REL_OID, meta_page, ColumnType::Int4).unwrap();
    (engine, index)
}

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

/// `btpo_flags` / slot count / pd_lsn of a page.
type PageState = (u8, usize, Lsn);

fn page_state(engine: &StorageEngine, page_id: PageId) -> PageState {
    let guard = engine.buffer_pool().pin(page_id).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    (
        BtreePage::flags(page).unwrap(),
        SlottedPage::slot_count(page),
        pg_storage::page::page_pd_lsn(page),
    )
}

/// Crash with only `BTreeSplitPrepare` durable, then recover: the undo pass
/// must finish the split end to end.
#[test]
fn test_recovery_incomplete_split_undo() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (meta_page, n, left, right) = {
        let (engine, index, n) = create_and_fill(tmp.path(), &config);
        let left = index.root_page();
        let st = index.split_prepare(left).unwrap();
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine);
        (index.meta_page(), n, st.left, st.right)
    };

    let (engine, index) = recover(tmp.path(), &config, meta_page);

    let (left_flags, left_slots, _) = page_state(&engine, left);
    let (_, right_slots, _) = page_state(&engine, right);
    assert_eq!(
        left_flags & BTREE_FLAG_SPLIT_INCOMPLETE,
        0,
        "undo must clear SPLIT_INCOMPLETE"
    );
    assert_eq!(
        left_slots + right_slots,
        n as usize,
        "the two halves must together hold every key exactly once"
    );
    assert!(
        right_slots > 0,
        "undo must move the upper half onto the right page"
    );

    assert_ne!(index.root_page(), left, "a root split needs a new root");
    assert_eq!(index.tree_level(), 1);
    assert_eq!(chain_from(&engine, left), vec![left, right]);
    assert_all_keys(&index, n);
    index
        .validate()
        .unwrap_or_else(|e| panic!("recovered tree must validate: {e}"));
    drop(engine);
}

/// Replaying the CLR must be a no-op. Recovering three times in a row (each
/// time abandoning the engine without a checkpoint, so the whole WAL including
/// the CLR is replayed again) must converge on identical pages.
///
/// This is the regression test for the CLR re-running its split copy: because
/// `apply_split_copy` appends to the right page, an unguarded replay moved the
/// upper half a second time and doubled the right page's entries.
#[test]
fn test_clr_idempotent() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (meta_page, n, left, right) = {
        let (engine, index, n) = create_and_fill(tmp.path(), &config);
        let left = index.root_page();
        let st = index.split_prepare(left).unwrap();
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine);
        (index.meta_page(), n, st.left, st.right)
    };

    let mut previous: Option<([PageState; 3], PageId)> = None;
    for round in 1..=3 {
        let (engine, index) = recover(tmp.path(), &config, meta_page);
        let root = index.root_page();
        let state = [
            page_state(&engine, left),
            page_state(&engine, right),
            page_state(&engine, root),
        ];

        assert_all_keys(&index, n);
        assert_eq!(index.tree_level(), 1, "round {round}");
        index
            .validate()
            .unwrap_or_else(|e| panic!("round {round} must validate: {e}"));

        if let Some((want_state, want_root)) = &previous {
            assert_eq!(&root, want_root, "round {round} moved the root");
            assert_eq!(
                &state, want_state,
                "round {round} diverged: replaying the CLR is not idempotent"
            );
        }
        previous = Some((state, root));

        std::mem::forget(engine); // crash again with the CLR in the WAL
    }
}

/// Crash with `BTreeSplitPrepare` + `BTreeSplitCopy` durable: redo already
/// moved the entries, so undo must finish the split *without* moving them
/// again, and the follow-up replay must stay idempotent too.
#[test]
fn test_clr_after_copy_crash() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (meta_page, n, left, right, copy_start_slot) = {
        let (engine, index, n) = create_and_fill(tmp.path(), &config);
        let left = index.root_page();
        let st = index.split_prepare(left).unwrap();
        index.split_copy(&st).unwrap();
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine);
        (index.meta_page(), n, st.left, st.right, st.copy_start_slot)
    };

    let (engine, index) = recover(tmp.path(), &config, meta_page);
    let (left_flags, left_slots, _) = page_state(&engine, left);
    let (_, right_slots, _) = page_state(&engine, right);
    assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
    assert_eq!(
        left_slots,
        copy_start_slot as usize,
        "the left page must keep exactly the entries below the split slot"
    );
    assert_eq!(
        right_slots,
        n as usize - copy_start_slot as usize,
        "the moved entries must appear on the right page exactly once"
    );
    assert_all_keys(&index, n);
    index
        .validate()
        .unwrap_or_else(|e| panic!("recovered tree must validate: {e}"));

    let root = index.root_page();
    let r1 = [
        page_state(&engine, left),
        page_state(&engine, right),
        page_state(&engine, root),
    ];
    std::mem::forget(engine);

    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_eq!(index.root_page(), root);
    assert_eq!(
        [
            page_state(&engine, left),
            page_state(&engine, right),
            page_state(&engine, root),
        ],
        r1,
        "replaying Prepare+Copy+CLR must converge"
    );
    assert_all_keys(&index, n);
    drop(engine);
}

/// The non-root shape: the split's downlink goes into an existing parent
/// instead of creating a new root, and the meta page is left alone.
#[test]
fn test_clr_non_root_split_undo() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (meta_page, n, root_before, left, right) = {
        let (engine, mut index, mut n) = create_and_fill(tmp.path(), &config);
        // Grow past the first root split so the leaf we break is not the root.
        while index.tree_level() == 0 {
            index.insert(&key(n), tid(n as u64)).unwrap();
            n += 1;
        }
        for _ in 0..50 {
            index.insert(&key(n), tid(n as u64)).unwrap();
            n += 1;
        }
        assert_eq!(index.tree_level(), 1);

        // Break the leftmost leaf, which now has a parent.
        let (leaf, _path, _) = index.descend_to_leaf(&key(0), &tid(0)).unwrap();
        assert_ne!(leaf, index.root_page(), "the target leaf must not be root");
        let st = index.split_prepare(leaf).unwrap();
        engine.wal_writer().flush().unwrap();
        let root_before = index.root_page();
        std::mem::forget(engine);
        (index.meta_page(), n, root_before, st.left, st.right)
    };

    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_eq!(
        index.root_page(),
        root_before,
        "a non-root split must not install a new root"
    );
    assert_eq!(index.tree_level(), 1, "the tree must not grow a level");

    let (left_flags, _, _) = page_state(&engine, left);
    assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
    assert_eq!(
        chain_from(&engine, left)[..2],
        [left, right],
        "the new right sibling must be spliced into the chain"
    );
    assert_all_keys(&index, n);
    index
        .validate()
        .unwrap_or_else(|e| panic!("recovered tree must validate: {e}"));

    // And the repair survives a second crash.
    let r1_left = page_state(&engine, left);
    let r1_right = page_state(&engine, right);
    std::mem::forget(engine);

    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_eq!(page_state(&engine, left), r1_left);
    assert_eq!(page_state(&engine, right), r1_right);
    assert_eq!(index.root_page(), root_before);
    assert_all_keys(&index, n);
    drop(engine);
}
