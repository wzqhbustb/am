//! Post-Stage-S review acceptance: HOT chain following without the 8-hop
//! cap (H1), CREATE INDEX bulk load over HOT chains (H2), the
//! hot-eligible-but-page-full cold fallback (B2), and crash recovery of an
//! UNCOMMITTED HOT update under a real CLOG (B3).
//!
//! Covered:
//!
//! - a 20-version HOT chain (the pre-H1 hardcoded 8-hop cap silently
//!   dropped versions past the eighth) is followed to its end by BOTH the
//!   heap scan and `index_lookup`;
//! - HOT-updating until the page is full falls back to a cross-page
//!   (non-HOT) update; the row stays visible and indexed afterwards (B2);
//! - an index built by bulk load over HOT-updated rows points at chain
//!   roots, so a later DELETE retires its entries without `EntryNotFound`
//!   and the tree stays consistent (H2);
//! - crash recovery with an uncommitted HOT transaction stamps the XID
//!   aborted via the real CLOG, making the OLD version visible again (B3).
//!
//! Acceptance: `cargo test -p pg-engine --test hot_chain`

use pg_engine::{Datum, Engine, EngineConfig, QueryResult};
use tempfile::TempDir;

fn open(dir: &std::path::Path) -> Engine {
    Engine::open(dir, EngineConfig::new(dir)).unwrap()
}

/// Open an engine with table `t (k1 INT, v INT)`, an index on `k1`, and the
/// single row `(1, 0)`. Updates that change only `v` are HOT-eligible.
fn open_with_indexed_row() -> (TempDir, Engine) {
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine.exec(None, "CREATE TABLE t (k1 INT, v INT)").unwrap();
    engine.exec(None, "INSERT INTO t VALUES (1, 0)").unwrap();
    engine.create_index("t", "k1").unwrap();
    (tmp, engine)
}

/// The single row's `(tid, v)` as seen by a fresh auto-commit scan.
fn scan_row(engine: &Engine) -> (pg_engine::Tid, i32) {
    let rows = engine.scan("t", None).unwrap();
    assert_eq!(rows.len(), 1, "exactly one row must be visible");
    let (tid, values) = rows.into_iter().next().unwrap();
    match &values[1] {
        Some(Datum::Int4(v)) => (tid, *v),
        other => panic!("unexpected v value: {other:?}"),
    }
}

/// H1: a HOT chain far deeper than the old 8-hop cap stays visible through
/// BOTH the scan and the index. 20 same-page updates (key column untouched)
/// build a 21-version chain; pre-H1 the versions past hop 8 vanished from
/// `index_lookup` (and from deep scans).
#[test]
fn hot_chain_deeper_than_8_hops_visible_via_scan_and_index() {
    const UPDATES: i32 = 20;
    let (_tmp, engine) = open_with_indexed_row();

    let (mut tid, _) = scan_row(&engine);
    let first_page = tid.page_id;
    for v in 1..=UPDATES {
        tid = engine
            .update("t", tid, &[Some(Datum::Int4(1)), Some(Datum::Int4(v))])
            .unwrap();
        assert_eq!(
            tid.page_id, first_page,
            "update {v} must stay on the same page (small tuples, empty page)"
        );
    }

    // Scan follows the chain to its end.
    let (scan_tid, v) = scan_row(&engine);
    assert_eq!(v, UPDATES, "scan must reach the chain tail");
    assert_eq!(scan_tid, tid);

    // index_lookup resolves the entry (which points at the chain ROOT) to
    // the visible tail version — the exact path the 8-hop cap broke.
    let found = engine
        .index_lookup("t", "k1", &Datum::Int4(1))
        .unwrap()
        .expect("the row must resolve through the index");
    assert_eq!(found, tid, "index_lookup must reach the chain tail");
    engine.shutdown();
}

/// H1 boundary + B2: keep HOT-updating until the page runs out of room. The
/// next update takes the cross-page cold fallback (hot_eligible=true but
/// `hot_applied` false — engine.rs `hot_applied` logic), and the row must
/// stay visible and indexed from its NEW page.
#[test]
fn hot_chain_reaching_page_capacity_falls_back_to_cold_update() {
    let (_tmp, engine) = open_with_indexed_row();

    let (mut tid, _) = scan_row(&engine);
    let first_page = tid.page_id;
    let mut v = 0;
    let mut crossed = false;
    // A page holds roughly a hundred of these small tuples; 400 iterations
    // is a generous, deterministic bound — if the fallback never happens
    // the test fails instead of hanging.
    for _ in 0..400 {
        v += 1;
        tid = engine
            .update("t", tid, &[Some(Datum::Int4(1)), Some(Datum::Int4(v))])
            .unwrap();
        if tid.page_id != first_page {
            crossed = true;
            break;
        }
    }
    assert!(crossed, "400 updates must overflow the first page");

    // The cold fallback performed index maintenance against the chain root:
    // the row is still visible and resolves through the index.
    let (scan_tid, scanned_v) = scan_row(&engine);
    assert_eq!(scanned_v, v);
    assert_eq!(scan_tid, tid);
    let found = engine
        .index_lookup("t", "k1", &Datum::Int4(1))
        .unwrap()
        .expect("the row must resolve through the index after the cold fallback");
    assert_eq!(found, tid);

    // And HOT resumes on the new page (key still unchanged).
    let tid2 = engine
        .update("t", tid, &[Some(Datum::Int4(1)), Some(Datum::Int4(v + 1))])
        .unwrap();
    assert_eq!(tid2.page_id, tid.page_id);
    let (_, scanned_v) = scan_row(&engine);
    assert_eq!(scanned_v, v + 1);
    engine.shutdown();
}

/// H2: CREATE INDEX bulk load over HOT-updated rows. The scan feeding the
/// bulk load yields the visible versions — HOT chain TAILS, which never own
/// index entries — so entries must be built against the chain ROOT, or the
/// first DELETE of such a row fails with `EntryNotFound` and leaks the
/// entry forever.
#[test]
fn create_index_over_hot_chains_then_delete_is_consistent() {
    const ROWS: i32 = 5;
    let tmp = TempDir::new().unwrap();
    let engine = open(tmp.path());
    engine
        .exec(None, "CREATE TABLE t (k1 INT, k2 INT, v INT)")
        .unwrap();
    for i in 0..ROWS {
        engine
            .exec(None, &format!("INSERT INTO t VALUES ({i}, {}, 0)", 100 + i))
            .unwrap();
    }
    // An index on k1 exists BEFORE the HOT updates (so the updates skip
    // index maintenance and form real HOT chains).
    engine.create_index("t", "k1").unwrap();

    // HOT-update every row (only the unindexed v changes), several times to
    // deepen the chains past trivial length.
    for round in 1..=3 {
        let rows = engine.scan("t", None).unwrap();
        assert_eq!(rows.len(), ROWS as usize);
        for (tid, values) in rows {
            let k1 = values[0].clone();
            let k2 = values[1].clone();
            engine
                .update("t", tid, &[k1, k2, Some(Datum::Int4(round))])
                .unwrap();
        }
    }

    // Bulk-load a SECOND index over the HOT chains (pre-H2 its entries
    // pointed at the chain tails).
    engine.create_index("t", "k2").unwrap();

    // DELETE every row: retires (k1, root) from the first index and
    // (k2, root) from the second — pre-H2 the k2 delete walked
    // `hot_chain_root` and then failed `EntryNotFound` on the tail-keyed
    // entry.
    let rows = engine.scan("t", None).unwrap();
    for (tid, _) in rows {
        engine.delete("t", tid).unwrap();
    }
    assert!(engine.scan("t", None).unwrap().is_empty());

    // Both indexes are empty and structurally valid.
    for i in 0..ROWS {
        assert!(
            engine
                .index_lookup("t", "k2", &Datum::Int4(100 + i))
                .unwrap()
                .is_none(),
            "deleted row {i} must not resolve through the k2 index"
        );
    }
    engine.btree_index("t", "k1").unwrap().validate().unwrap();
    engine.btree_index("t", "k2").unwrap().validate().unwrap();
    engine.shutdown();
}

/// B3: crash recovery with an UNCOMMITTED HOT transaction under the REAL
/// CLOG (not `NoOpClogAccessor`). The checkpoint pushes the HOT chain to
/// disk mid-transaction; recovery's undo stamps the XID aborted, so the OLD
/// version is visible again — via both the scan and the index.
#[test]
fn crash_recovery_uncommitted_hot_update_old_version_visible() {
    let tmp = TempDir::new().unwrap();
    {
        let engine = open(tmp.path());
        engine.exec(None, "CREATE TABLE t (k1 INT, v INT)").unwrap();
        engine.exec(None, "INSERT INTO t VALUES (1, 10)").unwrap();
        engine.create_index("t", "k1").unwrap();

        let txn = engine.begin_txn().unwrap();
        // HOT update (v is not indexed), never committed.
        let res = engine
            .exec(Some(&txn), "UPDATE t SET v = 20 WHERE k1 = 1")
            .unwrap();
        assert!(matches!(res, QueryResult::Affected(1)));
        // Push the chain (and the CLOG) to disk, then crash with the
        // transaction still in flight — no commit, no abort record.
        engine.checkpoint().unwrap();
        std::mem::forget(txn);
        std::mem::forget(engine);
    }

    let engine = open(tmp.path());
    // The uncommitted HOT update never took effect: the OLD version is
    // visible again.
    let (_, v) = scan_row(&engine);
    assert_eq!(v, 10, "the old version must be visible after recovery");
    assert!(
        engine
            .index_lookup("t", "k1", &Datum::Int4(1))
            .unwrap()
            .is_some(),
        "the row must resolve through the index after recovery"
    );
    engine.shutdown();
}
