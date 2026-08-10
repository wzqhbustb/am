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
use pg_am_btree::{btree_redo_handlers, BTreeAM, BTreeIndex, BTreeUndoHandler};

use pg_am_heap::slotted_page::SlottedPage;
use pg_am_heap::tuple::ColumnType;
use pg_am_heap::HeapUndoHandler;
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
    let engine = StorageEngine::open_with_redo_handlers(
        dir,
        config,
        btree_redo_handlers(),
        vec![
            Box::new(HeapUndoHandler),
            Box::new(BTreeUndoHandler),
        ],
    )
    .unwrap();
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

/// Read `btpo_flags` / slot count / pd_lsn of a page.
fn page_state(engine: &StorageEngine, page_id: PageId) -> (u8, usize, pg_storage::types::Lsn) {
    let guard = engine.buffer_pool().pin(page_id).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    (
        BtreePage::flags(page).unwrap(),
        SlottedPage::slot_count(page),
        pg_storage::page::page_pd_lsn(page),
    )
}

/// Crash with only `BTreeSplitPrepare` durable: the left page is marked
/// SPLIT_INCOMPLETE and linked to an initialized but empty right page.
/// Recovery redo replays Prepare; the undo handler finishes the split
/// (copy + downlink + commit-clear), so the recovered tree is fully valid.
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

    // Recovery 1: redo replays Prepare; undo finishes the split.
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_all_keys(&index, n);
    assert_ne!(index.root_page(), left, "undo must create a new root");
    assert_eq!(index.tree_level(), 1);
    assert_eq!(
        chain_from(&engine, left),
        vec![left, right],
        "the sibling chain must walk left -> right end to end"
    );
    let (left_flags, _, _) = page_state(&engine, left);
    assert_eq!(
        left_flags & BTREE_FLAG_SPLIT_INCOMPLETE,
        0,
        "undo must clear SPLIT_INCOMPLETE"
    );
    index
        .validate()
        .unwrap_or_else(|e| panic!("recovered tree must validate: {e}"));

    // Snapshot the post-undo page state so recovery 2 can be compared against
    // it: replaying the CLR must not move the entries a second time.
    let new_root = index.root_page();
    let r1_state = [
        page_state(&engine, left),
        page_state(&engine, right),
        page_state(&engine, new_root),
    ];

    std::mem::forget(engine); // crash again, mid/after recovery

    // Recovery 2: CLR replay is idempotent — same state.
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_all_keys(&index, n);
    assert_eq!(index.tree_level(), 1);
    assert_eq!(index.root_page(), new_root);
    assert_eq!(
        [
            page_state(&engine, left),
            page_state(&engine, right),
            page_state(&engine, new_root),
        ],
        r1_state,
        "replaying the CLR must converge on the state undo produced"
    );
    let (left_flags, _, _) = page_state(&engine, left);
    assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
    drop(engine);
}

/// Crash with `BTreeSplitPrepare` + `BTreeSplitCopy` durable: the left page
/// is truncated, the right page holds the upper half, but no downlink was
/// ever committed (and, being a root split, the new root was never
/// created). Recovery redo replays Prepare+Copy; the undo handler finishes
/// the split (downlink + commit-clear), so the recovered tree is fully valid.
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

    // Recovery 1: redo replays Prepare+Copy; undo finishes the split.
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_all_keys(&index, n);
    assert_ne!(index.root_page(), left, "undo must create a new root");
    assert_eq!(index.tree_level(), 1);
    assert_eq!(chain_from(&engine, left), vec![left, right]);
    let (left_flags, left_slots, _) = page_state(&engine, left);
    assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
    assert_eq!(left_slots, copy_start_slot as usize);
    let (_, right_slots, _) = page_state(&engine, right);
    assert_eq!(right_slots, n as usize - copy_start_slot as usize);
    std::mem::forget(engine); // crash again

    // Recovery 2: CLR replay is idempotent — same state.
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_all_keys(&index, n);
    assert_eq!(index.tree_level(), 1);
    let (left_flags, left_slots, _) = page_state(&engine, left);
    assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
    assert_eq!(left_slots, copy_start_slot as usize);
    let (_, right_slots, _) = page_state(&engine, right);
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
    let (left_flags, _, _) = page_state(&engine, left);
    assert_eq!(left_flags & BTREE_FLAG_SPLIT_INCOMPLETE, 0);
    // The root holds exactly two downlinks.
    let (_, root_slots, _) = page_state(&engine, index.root_page());
    assert_eq!(root_slots, 2);
    std::mem::forget(engine); // crash again

    // Recovery 2: idempotent re-replay of Prepare/Copy/Commit.
    let (engine, index) = recover(tmp.path(), &config, meta_page);
    assert_eq!(index.tree_level(), 1);
    assert_all_keys(&index, n);
    index.validate().unwrap();
    let (_, root_slots, _) = page_state(&engine, index.root_page());
    assert_eq!(root_slots, 2, "Commit redo must not duplicate the downlink");
    drop(engine);
}

// ---------------------------------------------------------------------
// Stage Q review (F1): delete's WAL record must name the re-validated page
// ---------------------------------------------------------------------

/// A delete that right-hops onto a split twin must WAL-log the TWIN's page
/// id, not the descent's stale one — redo keys records off `rec.page_id`,
/// so the stale id loses the delete (or removes an innocent entry).
///
/// Drive an inserter that keeps splitting the right-edge leaf while a
/// deleter removes EVERY OTHER key right at that frontier (oversized Text
/// keys: ~13 entries/leaf, so splits fire every few inserts and interpose
/// between the deleter's descent and its leaf latch with high probability),
/// then kill -9 and recover: every deleted key must STAY deleted and every
/// surviving key must be intact. (The online pre-crash state would pass
/// even with the bug — the wrong page id only bites at redo time, which is
/// why this test goes through a crash.)
///
/// Why every-other-key: deleting ALL of a leaf's entries leaves a page with
/// ≤1 live entries whose tuple bytes are dead space (M2b reclaims it only
/// via a split, and a split refuses < 2 entries) — a pre-existing Stage M
/// boundary, out of scope here. Deleting half keeps every leaf splittable
/// while exercising the same hop window.
#[test]
fn test_concurrent_split_delete_crash_recovers_deletions() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    const REL_OID_TEXT: Oid = Oid(16_389);
    /// ~600-byte keys: ~13 entries per 8 KB leaf, so the frontier splits
    /// constantly while the deleter works it.
    fn big_key(i: u64) -> Vec<u8> {
        let mut k = format!("{i:06}").into_bytes();
        k.resize(600, b'x');
        k
    }
    /// The deleter removes even race keys; odd ones survive.
    fn deleted(i: u64) -> bool {
        i % 2 == 0
    }

    const SEED: u64 = 120; // pre-existing keys (untouched by the race)
    const RACE: u64 = 240; // keys inserted at the frontier; half deleted

    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let meta_page = {
        let engine = Arc::new(StorageEngine::open(tmp.path(), &config).unwrap());
        let meta_page = {
            let am = BTreeAM::new(
                Arc::clone(engine.buffer_pool()),
                Arc::clone(engine.wal_writer()),
            );
            let mut index = am.create_index(REL_OID_TEXT, ColumnType::Text).unwrap();
            for i in 0..SEED {
                index.insert(&big_key(i), tid(i)).unwrap();
            }
            index.meta_page()
        };

        let watermark = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel();
        {
            let engine = Arc::clone(&engine);
            let watermark = Arc::clone(&watermark);
            thread::spawn(move || {
                let inserter = {
                    let engine = Arc::clone(&engine);
                    let watermark = Arc::clone(&watermark);
                    thread::spawn(move || {
                        let am = BTreeAM::new(
                            Arc::clone(engine.buffer_pool()),
                            Arc::clone(engine.wal_writer()),
                        );
                        let mut index = am
                            .open_index(REL_OID_TEXT, meta_page, ColumnType::Text)
                            .unwrap();
                        for i in 0..RACE {
                            let k = SEED + i;
                            index.insert(&big_key(k), tid(k)).unwrap();
                            watermark.store((k + 1) as usize, Ordering::SeqCst);
                        }
                    })
                };
                let deleter = {
                    let engine = Arc::clone(&engine);
                    let watermark = Arc::clone(&watermark);
                    thread::spawn(move || {
                        let am = BTreeAM::new(
                            Arc::clone(engine.buffer_pool()),
                            Arc::clone(engine.wal_writer()),
                        );
                        let mut index = am
                            .open_index(REL_OID_TEXT, meta_page, ColumnType::Text)
                            .unwrap();
                        for i in (0..RACE).filter(|i| deleted(*i)) {
                            let k = SEED + i;
                            // Delete right behind the insertion frontier:
                            // the target leaf is the one being split.
                            while (watermark.load(Ordering::SeqCst) as u64) <= k {
                                thread::yield_now();
                            }
                            index.delete(&big_key(k), tid(k)).unwrap();
                        }
                    })
                };
                inserter.join().unwrap();
                deleter.join().unwrap();
                let _ = tx.send(());
            });
        }
        rx.recv_timeout(Duration::from_secs(300))
            .expect("concurrent insert+delete deadlocked or ran too long");

        // Online sanity: deleted race keys are gone, survivors intact.
        {
            let am = BTreeAM::new(
                Arc::clone(engine.buffer_pool()),
                Arc::clone(engine.wal_writer()),
            );
            let index = am
                .open_index(REL_OID_TEXT, meta_page, ColumnType::Text)
                .unwrap();
            for i in 0..RACE {
                let k = SEED + i;
                let expect = if deleted(i) { None } else { Some(tid(k)) };
                assert_eq!(index.lookup(&big_key(k)).unwrap(), expect, "key {k}");
            }
            for i in 0..SEED {
                assert_eq!(index.lookup(&big_key(i)).unwrap(), Some(tid(i)));
            }
        }

        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine); // kill -9
        meta_page
    };

    // Recovery: redo replays the BTreeDelete records against the pages they
    // name. With the wrong page id (F1), a delete is lost (race key still
    // present) or mis-applied to an innocent slot (seed key missing).
    let (_engine, index) = recover(tmp.path(), &config, meta_page);
    let survivors: Vec<u64> = (0..SEED)
        .chain((0..RACE).filter(|i| !deleted(*i)).map(|i| SEED + i))
        .collect();
    let rows = index.range_scan(None, None).unwrap();
    assert_eq!(
        rows.len(),
        survivors.len(),
        "exactly the seed + odd race keys must survive recovery"
    );
    for (expect, (k, t)) in survivors.iter().zip(rows.iter()) {
        assert_eq!(k.as_slice(), big_key(*expect).as_slice());
        assert_eq!(*t, tid(*expect));
    }
    for i in (0..RACE).filter(|i| deleted(*i)) {
        assert_eq!(
            index.lookup(&big_key(SEED + i)).unwrap(),
            None,
            "deleted race key {} reappeared after recovery",
            SEED + i
        );
    }
    index.validate().unwrap();
}

// ---------------------------------------------------------------------
// Stage Q review (H2): root split on a freelist-recycled page needs an FPI
// ---------------------------------------------------------------------

/// A root split that reuses a FREELIST-RECYCLED page must recover cleanly.
/// The recycled page's on-disk image is its previous tenant's bytes
/// (`pd_upper != 0`), so without `create_new_root`'s post-image FPI the
/// seed `BTreeInsert`'s `init_if_fresh` would not fire and redo would apply
/// the insert onto garbage geometry (silently corrupting the root — or
/// failing recovery outright).
///
/// Drive: manufacture two "previous tenant" pages with heap-style geometry,
/// flush + free them, then fill the root leaf until the root split pops the
/// freelist (LIFO: the right twin gets one, the NEW ROOT gets the other).
/// kill -9, recover, and require a fully intact tree.
#[test]
fn test_root_split_on_recycled_page_crash_recovers() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (meta_page, n, recycled_root) = {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let am = BTreeAM::new(
            Arc::clone(engine.buffer_pool()),
            Arc::clone(engine.wal_writer()),
        );
        let mut index = am.create_index(REL_OID, ColumnType::Int4).unwrap();

        // Two "previous tenants": heap-initialized pages (pd_upper ==
        // PAGE_SIZE, no btree special space) made durable, then freed.
        // Freelist pop is LIFO: the split's right twin gets `victim2`, the
        // NEW ROOT gets `victim1`.
        let mut freed = Vec::new();
        for _ in 0..2 {
            let victim = {
                let mut guard = engine.buffer_pool().new_page().unwrap();
                SlottedPage::init(guard.page_mut().try_into().unwrap());
                guard.page_id()
            };
            engine.buffer_pool().flush(victim).unwrap();
            freed.push(victim);
        }
        {
            let mut allocator = engine.page_allocator().lock();
            for victim in &freed {
                allocator.free_page(*victim).unwrap();
            }
        }

        // Fill the root leaf; the next insert triggers the root split.
        const ENTRY_BYTES: usize = 4 + 10 + 4;
        let mut i = 0i32;
        while index.page_free_space(index.root_page()).unwrap() >= ENTRY_BYTES {
            index.insert(&key(i), tid(i as u64)).unwrap();
            i += 1;
        }
        index.insert(&key(i), tid(i as u64)).unwrap();
        let n = i + 1;
        assert_eq!(index.tree_level(), 1, "the root must have been promoted");
        assert_eq!(
            index.root_page(),
            freed[0],
            "the new root must be the recycled page (freelist is LIFO)"
        );
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine); // kill -9
        (index.meta_page(), n, freed[0])
    };

    // Recovery: without the FPI, redo's init_if_fresh sees the recycled
    // page's non-zero pd_upper and skips initialization — the seed insert
    // then lands on heap garbage geometry.
    let (_engine, index) = recover(tmp.path(), &config, meta_page);
    assert_eq!(index.root_page(), recycled_root);
    assert_eq!(index.tree_level(), 1);
    assert_all_keys(&index, n);
    index
        .validate()
        .unwrap_or_else(|e| panic!("recycled-root tree must validate after recovery: {e}"));
}
