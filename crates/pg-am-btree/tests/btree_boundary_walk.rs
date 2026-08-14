//! Regression tests for the two Stage-T final-review boundary bugs in the
//! B+Tree sibling-chain walk (follow-ups of the insert left-hop fix in
//! `btree_insert_left_hop.rs`).
//!
//! # Bug 1: pure DELETEs can drain a split twin into an empty-page black hole
//!
//! A duplicate run spans a split boundary (separator = K; L holds `(K, t1),
//! (K, t2)`, the twin E holds `(K, t3..)`). Pure DELETEs then empty E
//! completely (M2b has no page merge, so empty pages persist). A probe for K
//! is routed by `find_child` straight onto the empty E — which is never
//! "dominated" (the check is skipped for `slot_count == 0`) and never hops
//! right (an empty right neighbor owns nothing) — so `lookup(K)` false-
//! negatives and `delete(K, t1)` returns a spurious `EntryNotFound` even
//! though the entries sit on L. Fix: in LOCATE mode an empty leaf with a
//! valid `prev` hops left (the right check cannot bounce back from an empty
//! page, so this cannot cycle).
//!
//! # Bug 2: small-tid insert into a boundary run breaks chain order and
//! misses DuplicateKey
//!
//! After the run's entries on R are deleted (R's first key becomes K2 > K,
//! or R is empty), inserting `(K, t_new)` with `t_new` SMALLER than an
//! existing `(K, t_old)` on L lands at R slot 0 (no same-key left hop),
//! breaking the chain's full `(key, tid)` order (`L.last > R.first`) and
//! silently duplicating any `(K, tid)` that already exists on L (the
//! `DuplicateKey` check never sees it). Freelist reuse makes `t_new < t_old`
//! realistic. Fix: insert placement is decided by the FULL `(key, tid)`
//! chain position — when the landing page is empty or its first entry sorts
//! above the probe, the nearest non-empty LEFT sibling's LAST entry decides:
//! if it sorts above the probe, the entry belongs further left.

use std::sync::Arc;

use pg_am_btree::{BTreeAM, BTreeError, BTreeIndex};

use pg_am_heap::tuple::ColumnType;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid};

use tempfile::TempDir;

const REL_OID: Oid = Oid(16_403);

fn tid(i: u64) -> Tid {
    Tid {
        page_id: PageId(51_000 + i),
        slot_id: i as u16,
    }
}

fn key(i: i32) -> Vec<u8> {
    pg_am_btree::encode_i32(i).to_vec()
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

/// Bug 1: delete-driven empty twin — locate probes must still find the
/// entries sitting on the left sibling.
#[test]
fn locate_probes_survive_a_delete_drained_empty_twin() {
    let (_tmp, _engine, mut index) = setup();
    let k = key(7);
    // One duplicate run of key 7 large enough to span a split boundary.
    for i in 0..500u64 {
        index.insert(&k, tid(i)).unwrap();
    }
    assert!(index.tree_level() >= 1, "the run must have split the leaf");

    // Pure deletes drain the run's RIGHT portion completely: the twin ends
    // EMPTY while its separator downlink stays in the parent. (The split
    // point is byte-medial, so the right twin's tids are a suffix of the
    // run — deleting the 200..500 suffix removes all of them and some of
    // the left leaf's tail.)
    for i in 200..500u64 {
        index.delete(&k, tid(i)).unwrap();
    }

    // The probe for key 7 is routed by the parent straight onto the drained
    // (empty) twin. Pre-fix that black-holed: lookup false-negatived and
    // delete returned a spurious EntryNotFound.
    assert_eq!(
        index.lookup(&k).unwrap(),
        Some(tid(0)),
        "lookup must reach the born-left duplicates past the empty twin"
    );
    let all = index.lookup_all(&k).unwrap();
    let want: Vec<Tid> = (0..200u64).map(tid).collect();
    assert_eq!(all, want, "every surviving duplicate, in (key, tid) order");
    // Deleting an entry that sits LEFT of the empty twin must not be a
    // spurious EntryNotFound.
    index.delete(&k, tid(10)).unwrap();
    assert!(matches!(
        index.delete(&k, tid(10)),
        Err(BTreeError::EntryNotFound)
    ));
    assert_eq!(index.lookup_all(&k).unwrap().len(), 199);
    index.validate().unwrap();
}

/// Bug 2: inserting a SMALL tid into a boundary run whose right side lost
/// its key-K entries must place the entry on the left leaf (chain order +
/// DuplicateKey detection), never strand it at the right leaf's slot 0.
#[test]
fn insert_small_tid_into_boundary_run_keeps_chain_order() {
    let (_tmp, _engine, mut index) = setup();
    let k = key(7);
    for i in 0..500u64 {
        index.insert(&k, tid(i)).unwrap();
    }
    // A different key behind the run, so the right leaf's first key becomes
    // 8 once the run's right-side entries are deleted.
    for i in 0..10u64 {
        index.insert(&key(8), tid(1_000 + i)).unwrap();
    }
    // Drain the run's right side (same suffix argument as above): R's first
    // entry is now (8, ..) while L still holds (7, t0..t199).
    for i in 200..500u64 {
        index.delete(&k, tid(i)).unwrap();
    }

    // Re-inserting an exact (key, tid) pair that lives on L must be
    // DuplicateKey — pre-fix it was silently duplicated onto R.
    assert!(
        matches!(index.insert(&k, tid(100)), Err(BTreeError::DuplicateKey)),
        "the DuplicateKey check must reach the run on the left leaf"
    );

    // A NOT-present small tid (150 was deleted? no — 150 < 200 survives on
    // L; delete it first) must insert into the LEFT leaf: L.last =
    // (7, t199) sorts above (7, t150), so the chain position is inside L.
    index.delete(&k, tid(150)).unwrap();
    index.insert(&k, tid(150)).unwrap();
    assert_eq!(index.lookup(&k).unwrap(), Some(tid(0)));

    // A LARGE tid belongs to the right of the run: L.last = (7, t199) sorts
    // below (7, t50000), so it lands at the right leaf's slot 0 — chain
    // order preserved either way.
    index.insert(&k, tid(50_000)).unwrap();

    // Full (key, tid) order across the chain, no duplicates, no losses.
    let all = index.lookup_all(&k).unwrap();
    let mut want: Vec<Tid> = (0..200u64).map(tid).collect();
    want.push(tid(50_000));
    assert_eq!(all.len(), want.len(), "no silent duplicate/loss: {all:?}");
    assert_eq!(all, want);
    index.validate().unwrap();
}
