//! TEMP-DEBUG: deterministic single-threaded repro of the stale-prev
//! boundary misplacement behind the concurrent duplicate-run disorder.

use std::sync::Arc;

use pg_am_btree::{BTreeAM, BTreeIndex};

use pg_am_heap::tuple::ColumnType;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid};

use tempfile::TempDir;

const REL_OID: Oid = Oid(16_404);

fn tid(i: u64) -> Tid {
    Tid {
        page_id: PageId(9_000_000 + i / 60_000),
        slot_id: (i % 60_000) as u16,
    }
}

fn key(i: i32) -> Vec<u8> {
    pg_am_btree::key::encode_i32(i).to_vec()
}

#[test]
fn stale_prev_boundary_misplacement() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let am = BTreeAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let mut index: BTreeIndex = am.create_index(REL_OID, ColumnType::Int4).unwrap();

    // 106 entries: keys 0..=102 (tid = key) plus a key-51 run with big tids.
    // Slot layout: 0..=50 = keys 0..=50; 51 = (51,51); 52 = (51,9000);
    // 53 = (51,9001); 54 = (51,9002); 55..=105 = keys 52..=102.
    for i in 0..=102u64 {
        index.insert(&key(i as i32), tid(i)).unwrap();
    }
    for t in [9001u64, 9002] {
        index.insert(&key(51), tid(t)).unwrap();
    }
    index.insert(&key(51), tid(9000)).unwrap();
    // ^ inserts keep (key, tid) order: (51,51) < (51,9000) < (51,9001) < (51,9002)

    // Forced median split #1 (public crash-test steps): 106 entries split at
    // slot 53: L = slots 0..=52 (keys 0..=50 + (51,51) + (51,9000)),
    // R = [(51,9001), (51,9002), keys 52..=102], sep(R) = 51.
    let st = index.split_prepare(index.root_page()).unwrap();
    index.split_copy(&st).unwrap();
    let mut path = Vec::new();
    index.split_commit(&st, &mut path).unwrap();
    assert_eq!(index.tree_level(), 1);
    let left = st.left;

    // Forced median split #2 of L (53 entries, split at slot 26): M gets
    // keys 26..=50 plus (51,51) and (51,9000) — so M.last = (51,9000).
    // Chain L -> M -> R, but R.prev stays L (Prepare never re-points
    // old_next.prev): the stale link.
    let st2 = index.split_prepare(left).unwrap();
    index.split_copy(&st2).unwrap();
    let mut path2 = vec![index.root_page()];
    index.split_commit(&st2, &mut path2).unwrap();

    // The probe: (51,4000) sorts between M's (51,51) and M.last =
    // (51,9000) — the correct position is INSIDE M. Pre-fix the placement
    // walk consults R.prev = L (stale, skipping M): L.last = (25,25) <
    // (51,4000) -> STAY -> inserts into R's slot 0, leaving
    // M.last = (51,9000) > R.first = (51,4000): the run's chain order is
    // broken and lookup_all returns 9000 before 4000.
    index.insert(&key(51), tid(4000)).unwrap();

    let all = index.lookup_all(&key(51)).unwrap();
    let want = vec![tid(51), tid(4000), tid(9000), tid(9001), tid(9002)];
    assert_eq!(all, want, "key 51 run out of order across the boundary");
    index.validate().unwrap();
}
