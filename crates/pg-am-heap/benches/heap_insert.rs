//! End-to-end single-thread INSERT throughput for the heap AM (Stage I
//! acceptance: single-thread INSERT >= 30K ops/s over the pure heap AM path —
//! buffer pool + WAL append/flush, no TxnManager / CLOG layered on yet).
//!
//! Unlike `tuple_ops`, this drives the full `HeapAM::insert` path against a real
//! `StorageEngine` on a temp data dir, so it measures WAL append + flush + page
//! pinning, not just the in-memory slotted-page write.

use std::sync::Arc;

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use pg_am_heap::access_method::{AccessMethod, InsertContext, RelationDesc};
use pg_am_heap::tuple::{encode_tuple, ColumnType, Datum, TupleHeader};
use pg_am_heap::HeapAM;

use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid, TxnId};

use pg_txn::Snapshot;

use tempfile::TempDir;

const COLUMNS: [ColumnType; 2] = [ColumnType::Int4, ColumnType::Text];
const REL_OID: Oid = Oid(16_384);

fn encode_row(id: i32) -> Vec<u8> {
    let header = TupleHeader::new(
        TxnId(100),
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
        &[
            Some(Datum::Int4(id)),
            Some(Datum::Text(format!("row-{id}"))),
        ],
    )
    .unwrap()
}

fn bench_heap_insert(c: &mut Criterion) {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();
    let heap = HeapAM::new(
        Arc::clone(engine.buffer_pool()),
        Arc::clone(engine.wal_writer()),
    );
    let first_page = heap.create_heap(REL_OID).unwrap();

    let mut snap = Snapshot::everything();
    snap.current_xid = TxnId(100);

    // Pre-encode a pool of rows so the timed loop measures the pure insert
    // path (page acquire + WAL append + slotted write), not tuple encoding.
    let pool: Vec<Vec<u8>> = (0..1024).map(encode_row).collect();
    let mut next = 0usize;

    c.bench_function("heap_insert_e2e", |b| {
        b.iter(|| {
            let tuple = &pool[next % pool.len()];
            next = next.wrapping_add(1);
            heap.insert(InsertContext {
                rel: RelationDesc {
                    rel_oid: REL_OID,
                    first_page,
                    page_count: 1,
                    columns: &COLUMNS,
                },
                snapshot: &snap,
                tuple: black_box(tuple),
                out_tid: None,
            })
            .unwrap();
        })
    });
}

criterion_group!(benches, bench_heap_insert);
criterion_main!(benches);
