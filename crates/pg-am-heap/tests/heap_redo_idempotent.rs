//! Stage I redo idempotency: replaying heap records (and an FPI + heap record
//! sequence) any number of times yields the same page state.

use std::sync::Arc;

use pg_am_heap::tuple::{encode_tuple, ColumnType, Datum, TupleHeader};
use pg_am_heap::{HeapInsertHandler, HeapUpdateHandler, SlottedPage};

use pg_storage::clog::NoOpClogAccessor;
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::page::{page_pd_lsn, set_page_pd_lsn};
use pg_storage::positioned_file::PositionedFile;
use pg_storage::recovery::{
    ActiveXactTable, DirtyPageTable, FullPageImageRedoHandler, IncompleteSplitTracker,
    RedoContext, RedoHandler,
};
use pg_storage::types::{Lsn, PageId, Tid, TxnId, PAGE_SIZE};
use pg_storage::wal::record::WalRecord;

use tempfile::TempDir;

const COLUMNS: [ColumnType; 2] = [ColumnType::Int4, ColumnType::Text];

fn encode_row(id: i32, name: &str) -> Vec<u8> {
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
        &[Some(Datum::Int4(id)), Some(Datum::Text(name.to_string()))],
    )
    .unwrap()
}

#[test]
fn heap_insert_redo_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    // A freshly allocated (zeroed) heap page to replay onto.
    let page_id = {
        let guard = engine.buffer_pool().new_page().unwrap();
        guard.page_id()
    };

    let tuple = encode_row(7, "idempotent");
    let mut record = WalRecord::heap_insert(page_id, 0, tuple.clone(), TxnId(100)).unwrap();
    record.lsn = Lsn(1_000);

    let clog = NoOpClogAccessor;
    let mut att = ActiveXactTable::new();
    let mut dpt = DirtyPageTable::new();
    let handler = HeapInsertHandler;

    // Replay the same record ten times.
    for _ in 0..10 {
        let mut incomplete_splits = IncompleteSplitTracker::new();
        let mut ctx = RedoContext {
            buffer_pool: Some(engine.buffer_pool()),
            page_allocator: engine.page_allocator(),
            clog: &clog,
            att: &mut att,
            dpt: &mut dpt,
            incomplete_splits: &mut incomplete_splits,
        };
        handler.apply(&record, &mut ctx).unwrap();
    }

    // Exactly one tuple, at slot 0, with pd_lsn stamped to the record's LSN.
    let guard = engine.buffer_pool().pin(page_id).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(page_pd_lsn(page), Lsn(1_000));
    assert_eq!(SlottedPage::slot_count(page), 1);
    assert_eq!(SlottedPage::tuple(page, 0).unwrap(), Some(tuple.as_slice()));
}

#[test]
fn fpi_then_heap_record_idempotent() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    let page_id = {
        let guard = engine.buffer_pool().new_page().unwrap();
        guard.page_id()
    };

    // FPI baseline: an initialized, empty slotted page with pd_lsn = 0.
    let mut image = [0u8; PAGE_SIZE];
    SlottedPage::init(&mut image);
    set_page_pd_lsn(&mut image, Lsn(0));
    let mut fpi = WalRecord::full_page_image(page_id, image.to_vec()).unwrap();
    fpi.lsn = Lsn(100);

    let tuple = encode_row(42, "after-fpi");
    let mut insert = WalRecord::heap_insert(page_id, 0, tuple.clone(), TxnId(100)).unwrap();
    insert.lsn = Lsn(200);

    let data_file =
        Arc::new(PositionedFile::open(pg_storage::io::data_file_path(engine.data_dir())).unwrap());
    let fpi_handler = FullPageImageRedoHandler::new(data_file);
    let insert_handler = HeapInsertHandler;

    let clog = NoOpClogAccessor;
    let mut att = ActiveXactTable::new();
    let mut dpt = DirtyPageTable::new();

    // Replay the (FPI, HeapInsert) pair twice, simulating a crash mid-recovery.
    for _ in 0..2 {
        {
            let mut incomplete_splits = IncompleteSplitTracker::new();
            let mut ctx = RedoContext {
                buffer_pool: Some(engine.buffer_pool()),
                page_allocator: engine.page_allocator(),
                clog: &clog,
                att: &mut att,
                dpt: &mut dpt,
            incomplete_splits: &mut incomplete_splits,
            };
            fpi_handler.apply(&fpi, &mut ctx).unwrap();
        }
        {
            let mut incomplete_splits = IncompleteSplitTracker::new();
            let mut ctx = RedoContext {
                buffer_pool: Some(engine.buffer_pool()),
                page_allocator: engine.page_allocator(),
                clog: &clog,
                att: &mut att,
                dpt: &mut dpt,
            incomplete_splits: &mut incomplete_splits,
            };
            insert_handler.apply(&insert, &mut ctx).unwrap();
        }
    }

    // Final state: the FPI-restored page plus the one re-inserted tuple, with
    // pd_lsn advanced to the HeapInsert record's LSN.
    let guard = engine.buffer_pool().pin(page_id).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(page_pd_lsn(page), Lsn(200));
    assert_eq!(SlottedPage::slot_count(page), 1);
    assert_eq!(SlottedPage::tuple(page, 0).unwrap(), Some(tuple.as_slice()));
}

#[test]
fn heap_update_same_page_redo_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    let page_id = {
        let guard = engine.buffer_pool().new_page().unwrap();
        guard.page_id()
    };

    // Seed the page with the original version at slot 0 (lsn 1000).
    let original = encode_row(1, "original");
    let mut insert = WalRecord::heap_insert(page_id, 0, original.clone(), TxnId(100)).unwrap();
    insert.lsn = Lsn(1_000);

    // Same-page update: stamp slot 0 deleted + append the new version at slot 1.
    let updated = encode_row(1, "updated");
    let old_tid = Tid {
        page_id,
        slot_id: 0,
    };
    let new_tid = Tid {
        page_id,
        slot_id: 1,
    };
    let mut update =
        WalRecord::heap_update(old_tid, new_tid, TxnId(100), updated.clone(), TxnId(100)).unwrap();
    update.lsn = Lsn(2_000);

    let clog = NoOpClogAccessor;
    let mut att = ActiveXactTable::new();
    let mut dpt = DirtyPageTable::new();
    let insert_handler = HeapInsertHandler;
    let update_handler = HeapUpdateHandler;

    // Apply the insert once, then replay the update ten times.
    {
        let mut incomplete_splits = IncompleteSplitTracker::new();
        let mut ctx = RedoContext {
            buffer_pool: Some(engine.buffer_pool()),
            page_allocator: engine.page_allocator(),
            clog: &clog,
            att: &mut att,
            dpt: &mut dpt,
            incomplete_splits: &mut incomplete_splits,
        };
        insert_handler.apply(&insert, &mut ctx).unwrap();
    }
    for _ in 0..10 {
        let mut incomplete_splits = IncompleteSplitTracker::new();
        let mut ctx = RedoContext {
            buffer_pool: Some(engine.buffer_pool()),
            page_allocator: engine.page_allocator(),
            clog: &clog,
            att: &mut att,
            dpt: &mut dpt,
            incomplete_splits: &mut incomplete_splits,
        };
        update_handler.apply(&update, &mut ctx).unwrap();
    }

    // Two slots: old (stamped) at 0, new at 1, pd_lsn at the update's LSN.
    let guard = engine.buffer_pool().pin(page_id).unwrap();
    let page: &[u8; PAGE_SIZE] = guard.page().try_into().unwrap();
    assert_eq!(page_pd_lsn(page), Lsn(2_000));
    assert_eq!(SlottedPage::slot_count(page), 2);
    assert_eq!(
        SlottedPage::tuple(page, 1).unwrap(),
        Some(updated.as_slice())
    );
}

#[test]
fn heap_update_cross_page_redo_is_idempotent() {
    let tmp = TempDir::new().unwrap();
    let config = StorageConfig::new(tmp.path());
    let engine = StorageEngine::open(tmp.path(), &config).unwrap();

    let old_page = {
        let guard = engine.buffer_pool().new_page().unwrap();
        guard.page_id()
    };
    let new_page = {
        let guard = engine.buffer_pool().new_page().unwrap();
        guard.page_id()
    };

    // Seed the old version at old_page slot 0.
    let original = encode_row(1, "original");
    let mut insert = WalRecord::heap_insert(old_page, 0, original.clone(), TxnId(100)).unwrap();
    insert.lsn = Lsn(1_000);

    // Cross-page update: old_page slot 0 stamped, new version at new_page slot 0.
    let updated = encode_row(1, "updated");
    let old_tid = Tid {
        page_id: old_page,
        slot_id: 0,
    };
    let new_tid = Tid {
        page_id: new_page,
        slot_id: 0,
    };
    let mut update =
        WalRecord::heap_update(old_tid, new_tid, TxnId(100), updated.clone(), TxnId(100)).unwrap();
    update.lsn = Lsn(2_000);

    let clog = NoOpClogAccessor;
    let mut att = ActiveXactTable::new();
    let mut dpt = DirtyPageTable::new();
    let insert_handler = HeapInsertHandler;
    let update_handler = HeapUpdateHandler;

    {
        let mut incomplete_splits = IncompleteSplitTracker::new();
        let mut ctx = RedoContext {
            buffer_pool: Some(engine.buffer_pool()),
            page_allocator: engine.page_allocator(),
            clog: &clog,
            att: &mut att,
            dpt: &mut dpt,
            incomplete_splits: &mut incomplete_splits,
        };
        insert_handler.apply(&insert, &mut ctx).unwrap();
    }
    for _ in 0..10 {
        let mut incomplete_splits = IncompleteSplitTracker::new();
        let mut ctx = RedoContext {
            buffer_pool: Some(engine.buffer_pool()),
            page_allocator: engine.page_allocator(),
            clog: &clog,
            att: &mut att,
            dpt: &mut dpt,
            incomplete_splits: &mut incomplete_splits,
        };
        update_handler.apply(&update, &mut ctx).unwrap();
    }

    // Old page keeps the single stamped version; new page holds the new one.
    let old_guard = engine.buffer_pool().pin(old_page).unwrap();
    let old: &[u8; PAGE_SIZE] = old_guard.page().try_into().unwrap();
    assert_eq!(page_pd_lsn(old), Lsn(2_000));
    assert_eq!(SlottedPage::slot_count(old), 1);

    let new_guard = engine.buffer_pool().pin(new_page).unwrap();
    let new: &[u8; PAGE_SIZE] = new_guard.page().try_into().unwrap();
    assert_eq!(page_pd_lsn(new), Lsn(2_000));
    assert_eq!(SlottedPage::slot_count(new), 1);
    assert_eq!(
        SlottedPage::tuple(new, 0).unwrap(),
        Some(updated.as_slice())
    );
}
