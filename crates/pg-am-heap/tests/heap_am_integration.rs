//! Stage I integration: single-threaded heap CRUD + crash recovery.

use std::sync::Arc;

use pg_am_heap::access_method::{
    AccessMethod, DeleteContext, InsertContext, RelationDesc, ScanContext, UpdatableAM,
    UpdateContext, Vacuumable,
};
use pg_am_heap::tuple::{encode_tuple, ColumnType, Datum, TupleHeader};
use pg_am_heap::{heap_redo_handlers, HeapAM};

use pg_storage::clog::NoOpClogAccessor;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid, TxnId};

use pg_txn::Snapshot;

use tempfile::TempDir;

const COLUMNS: [ColumnType; 2] = [ColumnType::Int4, ColumnType::Text];
const REL_OID: Oid = Oid(16_384);

/// A snapshot whose own transaction is `xid` (so freshly inserted tuples carry
/// `t_xmin = xid`), otherwise "see everything committed".
fn writer_snapshot(xid: u64) -> Snapshot {
    let mut snap = Snapshot::everything();
    snap.current_xid = TxnId(xid);
    snap
}

/// Encode a `(Int4, Text)` row with the given inserting XID.
fn encode_row(xid: u64, id: i32, name: &str) -> Vec<u8> {
    let header = TupleHeader::new(
        TxnId(xid),
        TxnId::INVALID,
        0,
        [0u8; 16],
        Tid {
            page_id: PageId(0),
            slot_id: 0,
        },
        0,
    );
    encode_tuple(
        header,
        &COLUMNS,
        &[Some(Datum::Int4(id)), Some(Datum::Text(name.to_string()))],
    )
    .unwrap()
}

fn rel(first_page: PageId, page_count: u64) -> RelationDesc<'static> {
    RelationDesc {
        rel_oid: REL_OID,
        first_page,
        page_count,
        columns: &COLUMNS,
    }
}

#[test]
fn insert_scan_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );

    let first_page = heap.create_heap(REL_OID).unwrap();
    let snap = writer_snapshot(100);

    let n = 10;
    for i in 0..n {
        let tuple = encode_row(100, i, &format!("row-{i:03}"));
        let mut tid = Tid {
            page_id: PageId(0),
            slot_id: 0,
        };
        heap.insert(InsertContext {
            rel: rel(first_page, 1),
            snapshot: &snap,
            tuple: &tuple,
            out_tid: Some(&mut tid),
        })
        .unwrap();
        assert_eq!(tid.page_id, first_page);
        assert_eq!(tid.slot_id, i as u16);
    }

    let scan_snap = Snapshot::everything();
    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page, 1),
            snapshot: &scan_snap,
            clog: &NoOpClogAccessor,
        })
        .unwrap();
    assert_eq!(rows.len(), n as usize);
    for (i, (_tid, values)) in rows.iter().enumerate() {
        assert_eq!(values[0], Some(Datum::Int4(i as i32)));
        assert_eq!(values[1], Some(Datum::Text(format!("row-{i:03}"))));
    }
}

#[test]
fn update_then_delete_visibility() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );

    let first_page = heap.create_heap(REL_OID).unwrap();
    let snap = writer_snapshot(100);

    // Insert one row.
    let tuple = encode_row(100, 1, "original");
    let mut tid = Tid {
        page_id: PageId(0),
        slot_id: 0,
    };
    heap.insert(InsertContext {
        rel: rel(first_page, 1),
        snapshot: &snap,
        tuple: &tuple,
        out_tid: Some(&mut tid),
    })
    .unwrap();

    // Update it: old version becomes invisible, new version visible.
    let new_tuple = encode_row(100, 1, "updated");
    let mut new_tid = Tid {
        page_id: PageId(0),
        slot_id: 0,
    };
    heap.update(UpdateContext {
        rel: rel(first_page, 1),
        snapshot: &snap,
        old_tid: tid,
        new_tuple: &new_tuple,
        out_tid: Some(&mut new_tid),
    })
    .unwrap();
    assert_ne!(new_tid, tid, "update must produce a new TID");

    let scan_snap = Snapshot::everything();
    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page, 1),
            snapshot: &scan_snap,
            clog: &NoOpClogAccessor,
        })
        .unwrap();
    assert_eq!(rows.len(), 1, "only the new version is visible");
    assert_eq!(rows[0].1[1], Some(Datum::Text("updated".to_string())));
    assert_eq!(rows[0].0, new_tid);

    // Delete the new version: nothing visible.
    heap.delete(DeleteContext {
        rel: rel(first_page, 1),
        snapshot: &snap,
        tid: new_tid,
    })
    .unwrap();

    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page, 1),
            snapshot: &scan_snap,
            clog: &NoOpClogAccessor,
        })
        .unwrap();
    assert!(rows.is_empty(), "deleted row must be invisible");
}

#[test]
fn heap_crash_recovery_after_update() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let (first_page, new_tid) = {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let heap = HeapAM::new(
            Arc::clone(engine.buffer_pool()),
            Arc::clone(engine.wal_writer()),
        );
        let first_page = heap.create_heap(REL_OID).unwrap();
        let snap = writer_snapshot(100);

        let tuple = encode_row(100, 1, "original");
        let mut tid = Tid {
            page_id: PageId(0),
            slot_id: 0,
        };
        heap.insert(InsertContext {
            rel: rel(first_page, 1),
            snapshot: &snap,
            tuple: &tuple,
            out_tid: Some(&mut tid),
        })
        .unwrap();

        let new_tuple = encode_row(100, 1, "updated");
        let mut new_tid = Tid {
            page_id: PageId(0),
            slot_id: 0,
        };
        heap.update(UpdateContext {
            rel: rel(first_page, 1),
            snapshot: &snap,
            old_tid: tid,
            new_tuple: &new_tuple,
            out_tid: Some(&mut new_tid),
        })
        .unwrap();

        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine);
        (first_page, new_tid)
    };

    let engine =
        StorageEngine::open_with_redo_handlers(tmp.path(), &config, heap_redo_handlers()).unwrap();
    let heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let scan_snap = Snapshot::everything();
    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page, 1),
            snapshot: &scan_snap,
            clog: &NoOpClogAccessor,
        })
        .unwrap();
    assert_eq!(rows.len(), 1, "the updated row must survive the crash");
    assert_eq!(rows[0].1[1], Some(Datum::Text("updated".to_string())));
    assert_eq!(rows[0].0, new_tid);
}

#[test]
fn heap_crash_recovery() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    // Phase 1: insert rows, flush the WAL, then abandon the engine (crash)
    // WITHOUT a checkpoint so recovery must replay the heap WAL.
    let first_page = {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let heap = HeapAM::new(
            Arc::clone(engine.buffer_pool()),
            Arc::clone(engine.wal_writer()),
        );
        let first_page = heap.create_heap(REL_OID).unwrap();
        let snap = writer_snapshot(100);
        for i in 0..8 {
            let tuple = encode_row(100, i, &format!("v-{i}"));
            heap.insert(InsertContext {
                rel: rel(first_page, 1),
                snapshot: &snap,
                tuple: &tuple,
                out_tid: None,
            })
            .unwrap();
        }
        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine); // simulate kill -9: no graceful shutdown
        first_page
    };

    // Phase 2: reopen with heap redo handlers; replay must reconstruct the rows.
    let engine =
        StorageEngine::open_with_redo_handlers(tmp.path(), &config, heap_redo_handlers()).unwrap();
    let heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let scan_snap = Snapshot::everything();
    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page, 1),
            snapshot: &scan_snap,
            clog: &NoOpClogAccessor,
        })
        .unwrap();
    assert_eq!(rows.len(), 8, "all inserted rows must survive the crash");
    for (i, (_tid, values)) in rows.iter().enumerate() {
        assert_eq!(values[0], Some(Datum::Int4(i as i32)));
        assert_eq!(values[1], Some(Datum::Text(format!("v-{i}"))));
    }
}

/// Force the cross-page update path: fill the old page so the new version must
/// land on a freshly allocated page, then verify live visibility and that the
/// relocated row survives a crash via WAL replay.
#[test]
fn heap_cross_page_update_crash_recovery() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    // A ~5 KiB text: two of these cannot share one 8 KiB page, so updating the
    // target to a big value while a big filler occupies the page spills over.
    let big = "x".repeat(5000);

    let (first_page, new_tid) = {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let heap = HeapAM::new(
            Arc::clone(engine.buffer_pool()),
            Arc::clone(engine.wal_writer()),
        );
        let first_page = heap.create_heap(REL_OID).unwrap();
        let snap = writer_snapshot(100);

        // Target row (small) at slot 0 on the first page.
        let target = encode_row(100, 1, "original");
        let mut tid = Tid {
            page_id: PageId(0),
            slot_id: 0,
        };
        heap.insert(InsertContext {
            rel: rel(first_page, 1),
            snapshot: &snap,
            tuple: &target,
            out_tid: Some(&mut tid),
        })
        .unwrap();
        assert_eq!(tid.page_id, first_page);

        // Filler row (large) fills most of the first page.
        let filler = encode_row(100, 2, &big);
        heap.insert(InsertContext {
            rel: rel(first_page, 1),
            snapshot: &snap,
            tuple: &filler,
            out_tid: None,
        })
        .unwrap();

        // Update the target to a large value; it cannot fit on the old page, so
        // the new version must be placed on a newly allocated page.
        let new_tuple = encode_row(100, 1, &big);
        let mut new_tid = Tid {
            page_id: PageId(0),
            slot_id: 0,
        };
        heap.update(UpdateContext {
            rel: rel(first_page, 1),
            snapshot: &snap,
            old_tid: tid,
            new_tuple: &new_tuple,
            out_tid: Some(&mut new_tid),
        })
        .unwrap();
        assert_ne!(
            new_tid.page_id, first_page,
            "the new version must land on a different page (cross-page path)"
        );

        // Live view: the big-text update (id=1) and the filler (id=2) are both
        // visible; the original id=1 value is gone.
        let scan_snap = Snapshot::everything();
        let rows = heap
            .scan(ScanContext {
                rel: rel(first_page, 2),
                snapshot: &scan_snap,
                clog: &NoOpClogAccessor,
            })
            .unwrap();
        assert_eq!(rows.len(), 2);
        let updated = rows
            .iter()
            .find(|(_, v)| v[0] == Some(Datum::Int4(1)))
            .expect("updated row must be visible");
        assert_eq!(updated.1[1], Some(Datum::Text(big.clone())));
        assert_eq!(updated.0, new_tid);

        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine); // simulate kill -9
        (first_page, new_tid)
    };

    // Reopen and replay: the relocated row must be reconstructed on the new page.
    let engine =
        StorageEngine::open_with_redo_handlers(tmp.path(), &config, heap_redo_handlers()).unwrap();
    let heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let scan_snap = Snapshot::everything();
    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page, 2),
            snapshot: &scan_snap,
            clog: &NoOpClogAccessor,
        })
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "filler + relocated row must survive the crash"
    );
    let updated = rows
        .iter()
        .find(|(_, v)| v[0] == Some(Datum::Int4(1)))
        .expect("relocated row must survive the crash");
    assert_eq!(updated.1[1], Some(Datum::Text(big)));
    assert_eq!(updated.0, new_tid, "relocated row keeps its post-crash TID");
}

/// A delete targeting an illegal TID must be rejected AND must write nothing to
/// the WAL: otherwise a poison `HeapDelete` record would be replayed at recovery
/// and abort it. We provoke the rejected delete, flush + crash, then verify
/// recovery succeeds and the untouched live row is still visible.
#[test]
fn rejected_delete_leaves_no_poison_wal_record() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());

    let first_page = {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let heap = HeapAM::new(
            Arc::clone(engine.buffer_pool()),
            Arc::clone(engine.wal_writer()),
        );
        let first_page = heap.create_heap(REL_OID).unwrap();
        let snap = writer_snapshot(100);

        // One live row at slot 0.
        let tuple = encode_row(100, 1, "keep-me");
        heap.insert(InsertContext {
            rel: rel(first_page, 1),
            snapshot: &snap,
            tuple: &tuple,
            out_tid: None,
        })
        .unwrap();

        // Delete an out-of-range slot on the same page: must be rejected.
        let bad_tid = Tid {
            page_id: first_page,
            slot_id: 999,
        };
        let err = heap.delete(DeleteContext {
            rel: rel(first_page, 1),
            snapshot: &snap,
            tid: bad_tid,
        });
        assert!(err.is_err(), "delete of an illegal TID must be rejected");

        engine.wal_writer().flush().unwrap();
        std::mem::forget(engine); // simulate kill -9
        first_page
    };

    // Recovery must NOT choke on a poison HeapDelete record.
    let engine =
        StorageEngine::open_with_redo_handlers(tmp.path(), &config, heap_redo_handlers()).unwrap();
    let heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let scan_snap = Snapshot::everything();
    let rows = heap
        .scan(ScanContext {
            rel: rel(first_page, 1),
            snapshot: &scan_snap,
            clog: &NoOpClogAccessor,
        })
        .unwrap();
    assert_eq!(rows.len(), 1, "the untouched row must survive recovery");
    assert_eq!(rows[0].1[1], Some(Datum::Text("keep-me".to_string())));
}

/// `scan_dead_tuples` must be scoped to the relation it is given: a dead tuple
/// in one relation must never leak into another relation's result.
#[test]
fn scan_dead_tuples_is_relation_scoped() {
    const REL_A: Oid = Oid(20_001);
    const REL_B: Oid = Oid(20_002);

    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );

    let page_a = heap.create_heap(REL_A).unwrap();
    let page_b = heap.create_heap(REL_B).unwrap();
    let rel_a = RelationDesc {
        rel_oid: REL_A,
        first_page: page_a,
        page_count: 1,
        columns: &COLUMNS,
    };
    let rel_b = RelationDesc {
        rel_oid: REL_B,
        first_page: page_b,
        page_count: 1,
        columns: &COLUMNS,
    };
    let snap = writer_snapshot(100);

    // Insert two rows in each relation and delete the first of each.
    let dead = |rel: RelationDesc<'_>, id: i32| -> Tid {
        let mut tid = Tid {
            page_id: PageId(0),
            slot_id: 0,
        };
        let tuple = encode_row(100, id, "dead");
        heap.insert(InsertContext {
            rel,
            snapshot: &snap,
            tuple: &tuple,
            out_tid: Some(&mut tid),
        })
        .unwrap();
        let live = encode_row(100, id + 1, "live");
        heap.insert(InsertContext {
            rel,
            snapshot: &snap,
            tuple: &live,
            out_tid: None,
        })
        .unwrap();
        heap.delete(DeleteContext {
            rel,
            snapshot: &snap,
            tid,
        })
        .unwrap();
        tid
    };
    let dead_a = dead(rel_a, 1);
    let dead_b = dead(rel_b, 100);

    // oldest_xmin past the deleter (xid 100) so both deletes count as dead.
    let dead_in_a = heap
        .scan_dead_tuples(rel_a, TxnId(1_000), &NoOpClogAccessor)
        .unwrap();
    assert_eq!(
        dead_in_a,
        vec![dead_a],
        "relation A sees only its own dead tuple"
    );
    assert!(
        !dead_in_a.contains(&dead_b),
        "B's dead tuple must not leak into A"
    );

    let dead_in_b = heap
        .scan_dead_tuples(rel_b, TxnId(1_000), &NoOpClogAccessor)
        .unwrap();
    assert_eq!(
        dead_in_b,
        vec![dead_b],
        "relation B sees only its own dead tuple"
    );
    assert!(
        !dead_in_b.contains(&dead_a),
        "A's dead tuple must not leak into B"
    );
}
