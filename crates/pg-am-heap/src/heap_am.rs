//! Heap access method: single-threaded CRUD over slotted pages (M2a Stage I).
//!
//! [`HeapAM`] wires the in-memory page/tuple primitives to the storage engine's
//! buffer pool and WAL. Every page mutation follows the same discipline:
//!
//! 1. pin the page for write (`pin_mut` / `new_page`) — the content write latch
//!    serializes all mutators of that page and blocks flushes;
//! 2. append the WAL record and obtain its LSN;
//! 3. apply the change to the page;
//! 4. stamp the page's `pd_lsn` (authoritative, `page[0..8]`) to `record.lsn`;
//! 5. drop the guard, which marks the frame dirty.
//!
//! Because `flush_frame` takes a `content.read()` lock and fsyncs the WAL up to
//! the page's `pd_lsn` before writing, holding the write latch across steps 2–4
//! guarantees WAL-before-data: the record is durable before the dirty page can
//! reach disk.
//!
//! # Slot stability and logical delete
//!
//! Delete is *logical*: it stamps `t_xmax` on the tuple header and leaves the
//! line pointer `Normal`. It never calls [`SlottedPage::delete_tuple`] (which
//! recycles the slot as `Unused`), because MVCC still needs the physical row
//! and recycling would break TID stability. A consequence is that
//! [`SlottedPage::add_tuple`] always *appends* on a heap page (no `Unused` slot
//! to recycle), so the slot it returns is deterministically `slot_count`. Redo
//! relies on this to reproduce identical slots without a slot-addressed writer.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use pg_storage::buffer_pool::{BufferPool, PageGuardMut};
use pg_storage::clog::{ClogAccessor, TxnState};
use pg_storage::page::{page_pd_lsn, set_page_pd_lsn, PAGE_HEADER_SIZE};
use pg_storage::recovery::RedoHandler;
use pg_storage::types::{Lsn, Oid, PageId, Tid, TxnId, PAGE_SIZE};
use pg_storage::wal::record::WalRecord;
use pg_storage::wal::WalWriter;

use pg_txn::is_visible;

use crate::access_method::{
    AccessMethod, DeleteContext, InsertContext, RelationDesc, ScanContext, UpdatableAM,
    UpdateContext, Vacuumable,
};
use crate::error::{HeapError, Result};
use crate::line_pointer::{LpFlags, LINE_POINTER_SIZE};
use crate::redo::{HeapDeleteHandler, HeapInsertHandler, HeapUpdateHandler};
use crate::slotted_page::SlottedPage;
use crate::tuple::{decode_tuple, TupleHeader, HEAP_UPDATED, TUPLE_HEADER_SIZE};

/// Largest tuple that can ever fit on a page (page minus header minus one LP).
const MAX_TUPLE_BYTES: usize = PAGE_SIZE - PAGE_HEADER_SIZE - LINE_POINTER_SIZE;

/// Heap access method over the shared data file.
///
/// M2a has no relation→page map in the catalog, so `HeapAM` tracks, per
/// relation OID, the list of pages that hold its tuples. The list is seeded
/// lazily from [`RelationDesc::first_page`] / [`RelationDesc::page_count`] the
/// first time a relation is touched, then grows as inserts allocate new pages.
pub struct HeapAM {
    buffer_pool: Arc<BufferPool>,
    wal_writer: Arc<WalWriter>,
    /// Per-relation page lists (see the struct docs).
    pages: Mutex<HashMap<Oid, Vec<PageId>>>,
}

impl HeapAM {
    /// Create a heap AM bound to the engine's buffer pool and WAL writer.
    pub fn new(buffer_pool: Arc<BufferPool>, wal_writer: Arc<WalWriter>) -> Self {
        HeapAM {
            buffer_pool,
            wal_writer,
            pages: Mutex::new(HashMap::new()),
        }
    }

    /// Allocate and initialize a relation's first heap page, tracking it.
    ///
    /// Convenience for callers/tests that need to materialize a brand-new,
    /// empty heap before inserting. The `PageAlloc` record written by
    /// `new_page` extends the data file, so recovery can pin the page even if
    /// it was never flushed. The page's `init` is not separately WAL-logged;
    /// heap redo initializes a fresh page on demand.
    pub fn create_heap(&self, rel_oid: Oid) -> Result<PageId> {
        let page_id = {
            let mut guard = self.buffer_pool.new_page()?;
            let page = as_page_mut(&mut guard);
            SlottedPage::init(page);
            guard.page_id()
        };
        self.pages
            .lock()
            .expect("heap page map poisoned")
            .insert(rel_oid, vec![page_id]);
        Ok(page_id)
    }

    /// Return a snapshot of the pages tracked for `rel`, seeding from the
    /// relation descriptor on first touch.
    fn relation_pages(&self, rel: &RelationDesc<'_>) -> Vec<PageId> {
        let mut map = self.pages.lock().expect("heap page map poisoned");
        map.entry(rel.rel_oid)
            .or_insert_with(|| {
                (0..rel.page_count)
                    .map(|i| PageId(rel.first_page.0 + i))
                    .collect()
            })
            .clone()
    }

    /// Record that `page_id` now belongs to `rel_oid` (idempotent).
    fn track_page(&self, rel_oid: Oid, page_id: PageId) {
        let mut map = self.pages.lock().expect("heap page map poisoned");
        let list = map.entry(rel_oid).or_default();
        if !list.contains(&page_id) {
            list.push(page_id);
        }
    }

    /// Reject tuples that are empty or can never fit on a page, matching
    /// [`SlottedPage::add_tuple`]'s own guards but *before* any WAL is written.
    fn validate_tuple_len(bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() {
            return Err(HeapError::InvalidArgument(
                "cannot insert an empty tuple".to_string(),
            ));
        }
        if bytes.len() > MAX_TUPLE_BYTES {
            return Err(HeapError::TupleTooLarge(bytes.len()));
        }
        Ok(())
    }

    /// Pin a page (initialized) that has room for `needed` bytes, excluding
    /// `exclude`, allocating a fresh page if none of the relation's existing
    /// pages qualify. The returned guard is held for the caller's mutation.
    fn acquire_page_with_room(
        &self,
        rel: &RelationDesc<'_>,
        needed: usize,
        exclude: PageId,
    ) -> Result<PageGuardMut<'_>> {
        // Scan newest-first: a pure-append heap fills pages in allocation order,
        // so only the most recently allocated (tail) page still has room. A
        // front-to-back scan would re-pin every full page on each insert (O(n)
        // locks per insert, O(n^2) overall); reverse order finds room in O(1)
        // for the common append case.
        for page_id in self.relation_pages(rel).into_iter().rev() {
            if page_id == exclude {
                continue;
            }
            let mut guard = self.buffer_pool.pin_mut(page_id)?;
            {
                let page = as_page_mut(&mut guard);
                SlottedPage::init_if_fresh(page);
                if SlottedPage::free_space(page) >= needed {
                    return Ok(guard);
                }
            }
        }
        // No existing page has room: allocate a new one. A valid tuple always
        // fits on a freshly initialized page (validate_tuple_len bounds it), so
        // the caller can add_tuple unconditionally.
        let mut guard = self.buffer_pool.new_page()?;
        {
            let page = as_page_mut(&mut guard);
            SlottedPage::init(page);
        }
        self.track_page(rel.rel_oid, guard.page_id());
        Ok(guard)
    }

    /// Stamp `t_xmax` (and optionally the `HEAP_UPDATED` infomask bit) onto the
    /// live tuple at `tid`'s slot, in place. TID stability is preserved: the
    /// line pointer stays `Normal`.
    fn stamp_deleted(
        page: &mut [u8; PAGE_SIZE],
        tid: Tid,
        xmax: TxnId,
        updated: bool,
    ) -> Result<()> {
        let lp = SlottedPage::line_pointer(page, tid.slot_id)?;
        if lp.flags() != LpFlags::Normal {
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
}

impl AccessMethod for HeapAM {
    fn name(&self) -> &'static str {
        "heap"
    }

    fn insert(&self, ctx: InsertContext<'_>) -> Result<()> {
        let InsertContext {
            rel,
            snapshot,
            tuple,
            out_tid,
        } = ctx;
        Self::validate_tuple_len(tuple)?;
        // A tuple written with an INVALID writer XID would be invisible to
        // every scan forever (`is_effectively_committed` rejects INVALID on
        // sight) — a silent dead row. That is always a caller bug; catch it.
        debug_assert!(
            snapshot.current_xid != pg_storage::types::TxnId::INVALID,
            "heap insert with INVALID current_xid produces an unreadable tuple"
        );

        let needed = tuple.len() + LINE_POINTER_SIZE;
        let mut guard = self.acquire_page_with_room(&rel, needed, PageId::INVALID)?;
        let page_id = guard.page_id();
        let page = as_page_mut(&mut guard);

        // add_tuple always appends on a heap page (no Unused slots to recycle),
        // so the slot is known before the mutation — build the WAL record first.
        let slot = SlottedPage::slot_count(page) as u16;
        let rec = WalRecord::heap_insert(page_id, slot, tuple.to_vec(), snapshot.current_xid)?;
        let lsn = self.wal_writer.append(rec)?;
        let actual = SlottedPage::add_tuple(page, tuple)?;
        debug_assert_eq!(actual, slot, "heap slot prediction diverged from add_tuple");
        stamp_pd_lsn(page, lsn);

        if let Some(out) = out_tid {
            *out = Tid {
                page_id,
                slot_id: slot,
            };
        }
        Ok(())
    }

    fn scan(&self, ctx: ScanContext<'_>) -> Result<Vec<(Tid, Vec<Option<crate::tuple::Datum>>)>> {
        let clog = ctx.clog;
        let mut out = Vec::new();
        for page_id in self.relation_pages(&ctx.rel) {
            let guard = self.buffer_pool.pin(page_id)?;
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
            // A fresh (never-inserted) page has no tuples to yield.
            if SlottedPage::header(page).pd_upper == 0 {
                continue;
            }
            let slot_count = SlottedPage::slot_count(page) as u16;
            for slot in 0..slot_count {
                let Some(bytes) = SlottedPage::tuple(page, slot)? else {
                    continue;
                };
                let (header, values) = decode_tuple(bytes, ctx.rel.columns)?;
                if is_visible(header.t_xmin, header.t_xmax, ctx.snapshot, clog) {
                    out.push((
                        Tid {
                            page_id,
                            slot_id: slot,
                        },
                        values,
                    ));
                }
            }
        }
        Ok(out)
    }

    fn delete(&self, ctx: DeleteContext<'_>) -> Result<()> {
        let tid = ctx.tid;
        let xmax = ctx.snapshot.current_xid;

        let mut guard = self.buffer_pool.pin_mut(tid.page_id)?;
        let page = as_page_mut(&mut guard);

        // Validate the target is a live tuple BEFORE writing WAL: a rejected
        // delete must leave no HeapDelete record behind, or recovery would
        // decode a poison record whose own stamp_deleted fails and aborts
        // replay. This mirrors the pre-append check on the update path.
        {
            let lp = SlottedPage::line_pointer(page, tid.slot_id)?;
            if lp.flags() != LpFlags::Normal {
                return Err(HeapError::TupleNotFound(tid));
            }
        }

        let rec = WalRecord::heap_delete(tid, xmax, xmax)?;
        let lsn = self.wal_writer.append(rec)?;
        Self::stamp_deleted(page, tid, xmax, false)?;
        stamp_pd_lsn(page, lsn);
        Ok(())
    }

    fn redo_handlers(&self) -> Vec<Box<dyn RedoHandler>> {
        vec![
            Box::new(HeapInsertHandler),
            Box::new(HeapUpdateHandler),
            Box::new(HeapDeleteHandler),
        ]
    }
}

impl UpdatableAM for HeapAM {
    fn update(&self, ctx: UpdateContext<'_>) -> Result<()> {
        let UpdateContext {
            rel,
            snapshot,
            old_tid,
            new_tuple,
            out_tid,
        } = ctx;
        Self::validate_tuple_len(new_tuple)?;
        let xmax = snapshot.current_xid;
        let needed = new_tuple.len() + LINE_POINTER_SIZE;

        // Pin the old page first and verify the target tuple is live.
        let mut old_guard = self.buffer_pool.pin_mut(old_tid.page_id)?;
        {
            let old_page = as_page_mut(&mut old_guard);
            let lp = SlottedPage::line_pointer(old_page, old_tid.slot_id)?;
            if lp.flags() != LpFlags::Normal {
                return Err(HeapError::TupleNotFound(old_tid));
            }
        }

        // Fast path: the new version fits on the old page (single latch, single
        // page). Stamping the old tuple does not change slot_count, so the new
        // slot is `slot_count` and add_tuple appends there.
        let old_has_room = {
            let old_page = as_page_mut(&mut old_guard);
            SlottedPage::free_space(old_page) >= needed
        };

        if old_has_room {
            let page_id = old_guard.page_id();
            let old_page = as_page_mut(&mut old_guard);
            let new_slot = SlottedPage::slot_count(old_page) as u16;
            let new_tid = Tid {
                page_id,
                slot_id: new_slot,
            };
            let rec = WalRecord::heap_update(old_tid, new_tid, xmax, new_tuple.to_vec(), xmax)?;
            let lsn = self.wal_writer.append(rec)?;
            Self::stamp_deleted(old_page, old_tid, xmax, true)?;
            let actual = SlottedPage::add_tuple(old_page, new_tuple)?;
            debug_assert_eq!(actual, new_slot);
            stamp_pd_lsn(old_page, lsn);
            if let Some(out) = out_tid {
                *out = new_tid;
            }
            return Ok(());
        }

        // Cross-page: place the new version on another page while holding the
        // old page's latch. `acquire_page_with_room` excludes the old page so it
        // never re-pins it (which would deadlock on the content write lock).
        let mut new_guard = self.acquire_page_with_room(&rel, needed, old_tid.page_id)?;
        let new_page_id = new_guard.page_id();
        let new_slot = {
            let new_page = as_page_mut(&mut new_guard);
            SlottedPage::slot_count(new_page) as u16
        };
        let new_tid = Tid {
            page_id: new_page_id,
            slot_id: new_slot,
        };

        let rec = WalRecord::heap_update(old_tid, new_tid, xmax, new_tuple.to_vec(), xmax)?;
        let lsn = self.wal_writer.append(rec)?;

        {
            let old_page = as_page_mut(&mut old_guard);
            Self::stamp_deleted(old_page, old_tid, xmax, true)?;
            stamp_pd_lsn(old_page, lsn);
        }
        {
            let new_page = as_page_mut(&mut new_guard);
            let actual = SlottedPage::add_tuple(new_page, new_tuple)?;
            debug_assert_eq!(actual, new_slot);
            stamp_pd_lsn(new_page, lsn);
        }

        if let Some(out) = out_tid {
            *out = new_tid;
        }
        Ok(())
    }
}

impl Vacuumable for HeapAM {
    fn scan_dead_tuples(
        &self,
        rel: RelationDesc<'_>,
        oldest_xmin: TxnId,
        clog: &dyn ClogAccessor,
    ) -> Result<Vec<Tid>> {
        // A dead tuple is one of:
        //
        // 1. **Aborted inserter** (`t_xmin` aborted): the row was never
        //    visible to anyone and never will be, so it is dead regardless of
        //    `oldest_xmin`. Stage J made this reachable — before real aborts
        //    existed, no such rows could be produced. PG's vacuum reclaims
        //    aborted-insert tuples by the same rule.
        // 2. **Committed deleter** (`t_xmax` committed and older than
        //    `oldest_xmin`): no live snapshot can still see the row.
        //
        // The caller-supplied `clog` decides committedness authoritatively: a
        // tuple whose deleter aborted is NOT dead (the delete never took
        // effect). Only the tuple header is needed (xmin/xmax live at fixed
        // offsets), so no schema is required.
        let page_ids = self.relation_pages(&rel);

        let mut dead = Vec::new();
        for page_id in page_ids {
            let guard = self.buffer_pool.pin(page_id)?;
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
            if SlottedPage::header(page).pd_upper == 0 {
                continue;
            }
            let slot_count = SlottedPage::slot_count(page) as u16;
            for slot in 0..slot_count {
                let Some(bytes) = SlottedPage::tuple(page, slot)? else {
                    continue;
                };
                let header = match TupleHeader::read_from(bytes) {
                    Ok(h) => h,
                    // A single corrupted tuple must not abort the whole scan:
                    // vacuum is a background maintenance pass, so skip the
                    // unreadable slot and keep going (the corruption itself is
                    // surfaced loudly via the log).
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            %page_id,
                            slot,
                            "scan_dead_tuples: skipping undecodable tuple"
                        );
                        continue;
                    }
                };
                if clog.get_state(header.t_xmin) == TxnState::Aborted {
                    dead.push(Tid {
                        page_id,
                        slot_id: slot,
                    });
                    continue;
                }
                let xmax = header.t_xmax;
                if xmax != TxnId::INVALID
                    && xmax.0 < oldest_xmin.0
                    && clog.get_state(xmax) == TxnState::Committed
                {
                    dead.push(Tid {
                        page_id,
                        slot_id: slot,
                    });
                }
            }
        }
        Ok(dead)
    }
}

/// Reinterpret a write guard's page bytes as a fixed-size page array.
fn as_page_mut<'g>(guard: &'g mut PageGuardMut<'_>) -> &'g mut [u8; PAGE_SIZE] {
    guard
        .page_mut()
        .try_into()
        .expect("buffer frame is exactly PAGE_SIZE")
}

/// Advance the page's authoritative `pd_lsn` to `max(lsn, current)`.
///
/// A free function taking the value in a local first, so the mutable borrow for
/// the write does not overlap the immutable read of the current LSN.
fn stamp_pd_lsn(page: &mut [u8; PAGE_SIZE], lsn: Lsn) {
    let new_lsn = lsn.max(page_pd_lsn(page));
    set_page_pd_lsn(page, new_lsn);
}
