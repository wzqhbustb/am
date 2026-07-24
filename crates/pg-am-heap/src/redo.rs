//! Heap redo handlers (M2a Stage I).
//!
//! Three handlers replay the heap WAL records produced by [`crate::heap_am`]:
//! [`HeapInsertHandler`], [`HeapUpdateHandler`], [`HeapDeleteHandler`]. They are
//! stateless — the buffer pool and page allocator arrive via [`RedoContext`] —
//! so [`crate::heap_am::HeapAM::redo_handlers`] can hand fresh boxes to the
//! recovery registry (which `pg-storage` cannot construct itself, as it must
//! not depend on this crate).
//!
//! # Idempotency (tech-selection §11.6)
//!
//! Recovery replays from the checkpoint redo point and may re-run any prefix of
//! records after a crash *during* recovery. Each handler is therefore guarded by
//! the authoritative page LSN: if `page_pd_lsn(page) >= record.lsn`, the change
//! is already durable and the handler is a no-op. Otherwise it applies the
//! change and stamps `pd_lsn = max(record.lsn, pd_lsn)`. Because heap delete is
//! logical (slots are never recycled), [`SlottedPage::add_tuple`] appends
//! deterministically, so re-applying `HeapInsert` after a fresh page is
//! initialized reproduces the exact slot recorded in the WAL.
//!
//! Freshly allocated heap pages may be materialized as all-zero bytes when the
//! data file is extended during replay (the `PageAlloc` record, which has a
//! lower LSN, runs first). [`SlottedPage::init_if_fresh`] initializes such a
//! page before the first tuple is placed.

use crate::error::HeapError;
use crate::slotted_page::SlottedPage;
use crate::tuple::{TupleHeader, HEAP_UPDATED, TUPLE_HEADER_SIZE};
use pg_storage::buffer_pool::BufferPool;
use pg_storage::error::{Result, StorageError};
use pg_storage::page::{page_pd_lsn, set_page_pd_lsn};
use pg_storage::recovery::{RedoContext, RedoHandler};
use pg_storage::types::{Lsn, Tid, PAGE_SIZE};
use pg_storage::wal::record::{HeapDeleteRecord, HeapInsertRecord, HeapUpdateRecord, WalRecord};
use pg_storage::wal::WalRecordType;

/// The heap redo handlers, ready for injection into the recovery registry
/// before a crash-recovery replay (see `Engine::open_with_redo_handlers`).
///
/// `pg-storage` owns the registry but cannot depend on this crate, so the
/// caller opening the engine must pass these in.
pub fn heap_redo_handlers() -> Vec<Box<dyn RedoHandler>> {
    vec![
        Box::new(HeapInsertHandler),
        Box::new(HeapUpdateHandler),
        Box::new(HeapDeleteHandler),
    ]
}

/// Redo handler for `HeapInsert` records.
pub struct HeapInsertHandler;

impl RedoHandler for HeapInsertHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::HeapInsert
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = HeapInsertRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;
        let mut guard = pool.pin_mut(rec.page_id)?;
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().expect("frame is PAGE_SIZE");

        if page_pd_lsn(page) >= record.lsn {
            return Ok(());
        }
        SlottedPage::init_if_fresh(page);
        let slot = SlottedPage::add_tuple(page, &rec.tuple_bytes).map_err(heap_to_storage)?;
        // Slot divergence means the page on disk is inconsistent with the
        // WAL stream (e.g. torn base page). Hard-fail rather than silently
        // writing the tuple to the wrong slot (§11.6: redo never skips
        // silently).
        if slot != rec.slot_id {
            return Err(StorageError::MetadataCorrupted(format!(
                "HeapInsert redo slot diverged: record expects slot {}, page gives {slot}",
                rec.slot_id
            )));
        }
        stamp_pd_lsn(page, record.lsn);
        Ok(())
    }
}

/// Redo handler for `HeapUpdate` records (stamp old version + insert new one).
pub struct HeapUpdateHandler;

impl RedoHandler for HeapUpdateHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::HeapUpdate
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = HeapUpdateRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;

        // Same-page update (the fast path in `HeapAM::update`): both the old
        // version's t_xmax stamp and the new version's insert are logged under a
        // single LSN on one page. They MUST be applied under one pd_lsn guard —
        // stamping the old tuple first advances pd_lsn to record.lsn, so a
        // second, independently guarded block would (wrongly) skip the insert
        // and lose the new version on recovery.
        if rec.old_tid.page_id == rec.new_tid.page_id {
            let mut guard = pool.pin_mut(rec.old_tid.page_id)?;
            let page: &mut [u8; PAGE_SIZE] =
                guard.page_mut().try_into().expect("frame is PAGE_SIZE");
            if page_pd_lsn(page) < record.lsn {
                SlottedPage::init_if_fresh(page);
                stamp_deleted(page, rec.old_tid, rec.xmax_old, true).map_err(heap_to_storage)?;
                let slot =
                    SlottedPage::add_tuple(page, &rec.new_tuple_bytes).map_err(heap_to_storage)?;
                if slot != rec.new_tid.slot_id {
                    return Err(StorageError::MetadataCorrupted(format!(
                        "HeapUpdate redo slot diverged: record expects slot {}, page gives {slot}",
                        rec.new_tid.slot_id
                    )));
                }
                stamp_pd_lsn(page, record.lsn);
            }
            return Ok(());
        }

        // Cross-page update: the old and new versions live on different pages
        // that may have reached disk at different times before the crash, so
        // each is guarded independently.
        //
        // Old page: stamp t_xmax + HEAP_UPDATED.
        {
            let mut guard = pool.pin_mut(rec.old_tid.page_id)?;
            let page: &mut [u8; PAGE_SIZE] =
                guard.page_mut().try_into().expect("frame is PAGE_SIZE");
            if page_pd_lsn(page) < record.lsn {
                SlottedPage::init_if_fresh(page);
                stamp_deleted(page, rec.old_tid, rec.xmax_old, true).map_err(heap_to_storage)?;
                stamp_pd_lsn(page, record.lsn);
            }
        }

        // New page: append the new version.
        {
            let mut guard = pool.pin_mut(rec.new_tid.page_id)?;
            let page: &mut [u8; PAGE_SIZE] =
                guard.page_mut().try_into().expect("frame is PAGE_SIZE");
            if page_pd_lsn(page) < record.lsn {
                SlottedPage::init_if_fresh(page);
                let slot =
                    SlottedPage::add_tuple(page, &rec.new_tuple_bytes).map_err(heap_to_storage)?;
                if slot != rec.new_tid.slot_id {
                    return Err(StorageError::MetadataCorrupted(format!(
                        "HeapUpdate redo slot diverged: record expects slot {}, page gives {slot}",
                        rec.new_tid.slot_id
                    )));
                }
                stamp_pd_lsn(page, record.lsn);
            }
        }
        Ok(())
    }
}

/// Redo handler for `HeapDelete` records (logical delete: stamp t_xmax).
pub struct HeapDeleteHandler;

impl RedoHandler for HeapDeleteHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::HeapDelete
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = HeapDeleteRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;
        let mut guard = pool.pin_mut(rec.tid.page_id)?;
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().expect("frame is PAGE_SIZE");

        if page_pd_lsn(page) >= record.lsn {
            return Ok(());
        }
        stamp_deleted(page, rec.tid, rec.xmax, false).map_err(heap_to_storage)?;
        stamp_pd_lsn(page, record.lsn);
        Ok(())
    }
}

/// Recovery always opens the buffer pool before replay (Stage I reorder), so a
/// missing pool is a programming error rather than a recoverable condition.
fn require_pool<'a>(ctx: &RedoContext<'a>) -> Result<&'a BufferPool> {
    ctx.buffer_pool.ok_or_else(|| {
        StorageError::InvalidOperation(
            "heap redo requires a buffer pool in RedoContext".to_string(),
        )
    })
}

/// Advance the page's authoritative `pd_lsn` to `max(lsn, current)`, sequencing
/// the read before the write to satisfy the borrow checker.
fn stamp_pd_lsn(page: &mut [u8; PAGE_SIZE], lsn: Lsn) {
    let new_lsn = lsn.max(page_pd_lsn(page));
    set_page_pd_lsn(page, new_lsn);
}

/// Stamp `t_xmax` (and optionally `HEAP_UPDATED`) onto the tuple at `tid`'s
/// slot, mirroring `HeapAM::stamp_deleted` for the redo path.
fn stamp_deleted(
    page: &mut [u8; PAGE_SIZE],
    tid: Tid,
    xmax: pg_storage::types::TxnId,
    updated: bool,
) -> std::result::Result<(), HeapError> {
    let lp = SlottedPage::line_pointer(page, tid.slot_id)?;
    if lp.flags() != crate::line_pointer::LpFlags::Normal {
        return Err(HeapError::TupleNotFound(tid));
    }
    let off = lp.off() as usize;
    let mut header = TupleHeader::read_from(&page[off..off + TUPLE_HEADER_SIZE])?;
    header.t_xmax = xmax;
    if updated {
        header.t_infomask |= HEAP_UPDATED;
    }
    header.write_to(&mut page[off..off + TUPLE_HEADER_SIZE]);
    Ok(())
}

/// Map a heap-layer error into a storage error for the redo dispatch. A heap
/// failure during redo (e.g. a page that cannot hold a logged tuple) indicates
/// on-disk inconsistency, not a routine condition.
fn heap_to_storage(e: HeapError) -> StorageError {
    match e {
        HeapError::Storage(s) => s,
        other => StorageError::MetadataCorrupted(format!("heap redo: {other}")),
    }
}
