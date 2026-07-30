//! B+Tree redo handlers (tech-selection §13.3, §11.6).
//!
//! Five stateless handlers replay the records produced by
//! [`crate::index::BTreeIndex`]: [`BTreeInsertHandler`],
//! [`BTreeDeleteHandler`] and the three split handlers. They follow the heap
//! redo style: no owned state (the buffer pool arrives via [`RedoContext`]),
//! idempotent under the authoritative page LSN, and every inconsistency is a
//! hard failure (`StorageError::MetadataCorrupted`), never a silent skip of
//! something the record says must exist.
//!
//! # Idempotency anchors
//!
//! - `BTreeInsert` / `BTreeDelete`: `page.pd_lsn >= record.lsn` skips.
//! - `BTreeSplitPrepare`: both pages guarded independently by
//!   `pd_lsn < record.lsn`. The right page is **fully re-initialized** (any
//!   previous tenant's bytes on a recycled page are overwritten — every such
//!   earlier write has a lower LSN than the Prepare record). The left page's
//!   pre-Prepare `btpo_next` cannot be re-read from the left page once its
//!   post-Prepare image is durable, so it travels in the payload as
//!   `left_old_next`.
//! - `BTreeSplitCopy`: applies only while
//!   `left_page.pd_lsn == left_page_pre_lsn` (§13.3 P2-9). The moved entries
//!   are **recomputed** from the left page's pre-copy image; the payload
//!   stays O(20 bytes). The online path flushes the right page's post-copy
//!   image before releasing the left page's latch, so "left truncated and
//!   durable, right not" cannot occur; if it ever does, redo hard-fails
//!   rather than recover an empty right page.
//! - `BTreeSplitCommit`: the parent downlink is inserted only while
//!   `parent_page.pd_lsn < record.lsn` (the downlink is logged by exactly
//!   one record — the Commit — so this guard is precise); the left page's
//!   `SPLIT_INCOMPLETE`/`ROOT` clear is guarded the same way.
//!
//! Freshly allocated pages materialize as zeros during replay (their
//! `PageAlloc` record runs first). `BTreeInsert` redo initializes such a
//! page from the record's `level`/`flags` fields — this is how a new root
//! created by a root split recovers without any separate init record.

use pg_am_heap::slotted_page::SlottedPage;
use pg_storage::buffer_pool::BufferPool;
use pg_storage::error::{Result, StorageError};
use pg_storage::page::{page_pd_lsn, set_page_pd_lsn};
use pg_storage::recovery::{RedoContext, RedoHandler};
use pg_storage::types::{Lsn, PAGE_SIZE};
use pg_storage::wal::record::{
    BTreeDeleteRecord, BTreeInsertRecord, BTreeSplitCommitRecord, BTreeSplitCopyRecord,
    BTreeSplitPrepareRecord, WalRecord,
};
use pg_storage::wal::WalRecordType;

use crate::error::BTreeError;
use crate::index::apply_split_copy;
use crate::page::{self, BtreePage};

/// The B+Tree redo handlers, ready for injection into the recovery registry
/// before a crash-recovery replay (see `Engine::open_with_redo_handlers`).
pub fn btree_redo_handlers() -> Vec<Box<dyn RedoHandler>> {
    vec![
        Box::new(BTreeInsertHandler),
        Box::new(BTreeDeleteHandler),
        Box::new(BTreeSplitPrepareHandler),
        Box::new(BTreeSplitCopyHandler),
        Box::new(BTreeSplitCommitHandler),
    ]
}

/// Redo handler for `BTreeInsert`: leaf/internal entry inserts, new-root
/// seeds, and meta-page root records.
pub struct BTreeInsertHandler;

impl RedoHandler for BTreeInsertHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::BTreeInsert
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = BTreeInsertRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;
        let mut guard = pool.pin_mut(rec.page_id)?;
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().expect("frame is PAGE_SIZE");

        if page_pd_lsn(page) >= record.lsn {
            return Ok(());
        }
        // A zero page is fresh (materialized by PageAlloc during replay);
        // initialize it with the level/flags the record carries.
        BtreePage::init_if_fresh(page, rec.level, rec.flags);
        BtreePage::insert_entry_at(page, rec.slot_id, &rec.tuple_bytes)
            .map_err(btree_to_storage)?;
        stamp_pd_lsn(page, record.lsn);
        Ok(())
    }
}

/// Redo handler for `BTreeDelete`: physical removal of one index entry.
pub struct BTreeDeleteHandler;

impl RedoHandler for BTreeDeleteHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::BTreeDelete
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = BTreeDeleteRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;
        let mut guard = pool.pin_mut(rec.page_id)?;
        let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().expect("frame is PAGE_SIZE");

        if page_pd_lsn(page) >= record.lsn {
            return Ok(());
        }
        BtreePage::remove_entry_at(page, rec.slot_id).map_err(btree_to_storage)?;
        stamp_pd_lsn(page, record.lsn);
        Ok(())
    }
}

/// Redo handler for `BTreeSplitPrepare` (§13.3 step 1).
pub struct BTreeSplitPrepareHandler;

impl RedoHandler for BTreeSplitPrepareHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::BTreeSplitPrepare
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = BTreeSplitPrepareRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;

        // Right page: full re-initialization (see the module docs).
        {
            let mut guard = pool.pin_mut(rec.new_right_page)?;
            let page: &mut [u8; PAGE_SIZE] =
                guard.page_mut().try_into().expect("frame is PAGE_SIZE");
            if page_pd_lsn(page) < record.lsn {
                BtreePage::init_right_page(page, rec.left_page, rec.left_old_next, rec.level);
                stamp_pd_lsn(page, record.lsn);
            }
        }

        // Left page: mark split-incomplete and link to the new right page.
        {
            let mut guard = pool.pin_mut(rec.left_page)?;
            let page: &mut [u8; PAGE_SIZE] =
                guard.page_mut().try_into().expect("frame is PAGE_SIZE");
            if page_pd_lsn(page) < record.lsn {
                BtreePage::init_if_fresh(page, rec.level, BtreePage::flags_for_level(rec.level));
                // §13.3: high_key_bytes is a redo validation marker — the
                // left page's pre-split maximum key must match it.
                let count = SlottedPage::slot_count(page) as u16;
                if count > 0 {
                    let bytes = SlottedPage::tuple(page, count - 1)
                        .map_err(BTreeError::Heap)
                        .map_err(btree_to_storage)?
                        .ok_or_else(|| {
                            StorageError::MetadataCorrupted(format!(
                                "split prepare redo: left page {} last slot unreadable",
                                rec.left_page
                            ))
                        })?;
                    let key = if rec.level == 0 {
                        page::decode_leaf_entry(bytes).map_err(btree_to_storage)?.0
                    } else {
                        page::decode_internal_entry(bytes)
                            .map_err(btree_to_storage)?
                            .0
                    };
                    if key != rec.high_key_bytes.as_slice() {
                        return Err(StorageError::MetadataCorrupted(format!(
                            "split prepare redo: left page {} high key diverged from record",
                            rec.left_page
                        )));
                    }
                }
                BtreePage::apply_prepare_left(page, rec.new_right_page)
                    .map_err(btree_to_storage)?;
                stamp_pd_lsn(page, record.lsn);
            }
        }
        Ok(())
    }
}

/// Redo handler for `BTreeSplitCopy` (§13.3 step 2, minimal payload).
pub struct BTreeSplitCopyHandler;

impl RedoHandler for BTreeSplitCopyHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::BTreeSplitCopy
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = BTreeSplitCopyRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;

        let mut left_guard = pool.pin_mut(rec.left_page)?;
        let left_lsn = page_pd_lsn(left_guard.page());
        if left_lsn != rec.left_page_pre_lsn {
            // The left page is NOT the pre-copy image the anchor expects.
            // The only consistent explanation is that the copy already
            // happened (fully replayed, or the right page flushed before the
            // crash while the left was not): then the RIGHT page must hold
            // the moved entries. If it does not, the WAL stream and the
            // pages disagree — hard-fail loudly, never a silent skip (§11.6,
            // v2.3-24).
            let mut right_guard = pool.pin_mut(rec.right_page)?;
            if page_pd_lsn(right_guard.page()) < record.lsn {
                return Err(StorageError::MetadataCorrupted(format!(
                    "split copy redo: left page {} is not the pre-copy image (pd_lsn {:?} != \
                     anchor {:?}) and right page {} lacks the copy (pd_lsn {:?} < record lsn {:?})",
                    rec.left_page,
                    left_lsn,
                    rec.left_page_pre_lsn,
                    rec.right_page,
                    page_pd_lsn(right_guard.page()),
                    record.lsn
                )));
            }
            // Right holds the copy: rebuild the left page from its current
            // content. If the copy was already fully applied this rebuild is
            // a deterministic no-op (the same kept entries are re-packed);
            // if only the left page's truncation was lost, this restores it.
            let left_page: &mut [u8; PAGE_SIZE] = left_guard
                .page_mut()
                .try_into()
                .expect("frame is PAGE_SIZE");
            let right_page: &mut [u8; PAGE_SIZE] = right_guard
                .page_mut()
                .try_into()
                .expect("frame is PAGE_SIZE");
            apply_split_copy(left_page, right_page, rec.copy_start_slot, false)
                .map_err(btree_to_storage)?;
            stamp_pd_lsn(left_page, record.lsn);
            return Ok(());
        }

        // Recompute the moved entries from the left page's pre-copy image.
        let mut right_guard = pool.pin_mut(rec.right_page)?;
        let right_lsn = page_pd_lsn(right_guard.page());
        {
            let left_page: &mut [u8; PAGE_SIZE] = left_guard
                .page_mut()
                .try_into()
                .expect("frame is PAGE_SIZE");
            let right_page: &mut [u8; PAGE_SIZE] = right_guard
                .page_mut()
                .try_into()
                .expect("frame is PAGE_SIZE");
            // When the right page's post-copy image is already durable it
            // holds the moved entries; only the left page's rebuild is
            // missing then. Both branches rebuild the left page, so redo
            // always converges to the online path's byte-identical result.
            let move_to_right = right_lsn < record.lsn;
            apply_split_copy(left_page, right_page, rec.copy_start_slot, move_to_right)
                .map_err(btree_to_storage)?;
            if move_to_right {
                stamp_pd_lsn(right_page, record.lsn);
            }
            stamp_pd_lsn(left_page, record.lsn);
        }
        Ok(())
    }
}

/// Redo handler for `BTreeSplitCommit` (§13.3 step 3).
pub struct BTreeSplitCommitHandler;

impl RedoHandler for BTreeSplitCommitHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::BTreeSplitCommit
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = BTreeSplitCommitRecord::decode(&record.payload)?;
        let pool = require_pool(ctx)?;

        // Parent: insert the downlink `(separator_key, right_page)`.
        {
            let mut guard = pool.pin_mut(rec.parent_page)?;
            let page: &mut [u8; PAGE_SIZE] =
                guard.page_mut().try_into().expect("frame is PAGE_SIZE");
            if page_pd_lsn(page) < record.lsn {
                if SlottedPage::header(page).pd_upper == 0 {
                    // The parent must have been initialized by an earlier
                    // record (a fresh root's seed `BTreeInsert`, or the
                    // parent's own split records) — replaying in LSN order
                    // guarantees it ran first.
                    return Err(StorageError::MetadataCorrupted(format!(
                        "split commit redo: parent page {} has no prior image",
                        rec.parent_page
                    )));
                }
                let entry = page::encode_internal_entry(&rec.separator_key, rec.right_page);
                BtreePage::insert_entry_at(page, rec.parent_insert_slot, &entry)
                    .map_err(btree_to_storage)?;
                stamp_pd_lsn(page, record.lsn);
            }
        }

        // Left: clear SPLIT_INCOMPLETE (and ROOT, for a root split).
        {
            let mut guard = pool.pin_mut(rec.left_page)?;
            let page: &mut [u8; PAGE_SIZE] =
                guard.page_mut().try_into().expect("frame is PAGE_SIZE");
            if page_pd_lsn(page) < record.lsn {
                if SlottedPage::header(page).pd_upper == 0 {
                    return Err(StorageError::MetadataCorrupted(format!(
                        "split commit redo: left page {} has no prior image",
                        rec.left_page
                    )));
                }
                BtreePage::apply_commit_left(page).map_err(btree_to_storage)?;
                stamp_pd_lsn(page, record.lsn);
            }
        }
        Ok(())
    }
}

/// Recovery always opens the buffer pool before replay (Stage I reorder), so
/// a missing pool is a programming error rather than a recoverable
/// condition.
fn require_pool<'a>(ctx: &RedoContext<'a>) -> Result<&'a BufferPool> {
    ctx.buffer_pool.ok_or_else(|| {
        StorageError::InvalidOperation(
            "btree redo requires a buffer pool in RedoContext".to_string(),
        )
    })
}

/// Advance the page's authoritative `pd_lsn` to `max(lsn, current)`.
fn stamp_pd_lsn(page: &mut [u8; PAGE_SIZE], lsn: Lsn) {
    let new_lsn = lsn.max(page_pd_lsn(page));
    set_page_pd_lsn(page, new_lsn);
}

/// Map a B+Tree-layer error into a storage error for the redo dispatch. A
/// B+Tree failure during redo (a page that cannot hold a logged entry, a
/// diverged slot) indicates on-disk inconsistency, not a routine condition.
fn btree_to_storage(e: BTreeError) -> StorageError {
    match e {
        BTreeError::Storage(s) => s,
        other => StorageError::MetadataCorrupted(format!("btree redo: {other}")),
    }
}
