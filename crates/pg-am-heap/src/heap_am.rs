//! Heap access method: single-threaded CRUD over slotted pages (M2a Stage I),
//! with the Stage K page chain and AM-internal `t_xmin` stamping.
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
//! # The page chain (Stage K)
//!
//! Every user heap page carries [`HEAP_SPECIAL_SIZE`] bytes of special space
//! holding a forward pointer (`SlottedPage::set_next_page` / `next_page`), so
//! a relation's pages form a singly linked chain headed at
//! `RelationDesc::first_page`. The per-relation page list kept in memory is
//! only a **cache**: it is rebuilt by walking the chain
//! (`seed_from_chain`) the first time a relation is touched after open, then
//! grows as inserts extend the chain. Chain extension appends a freshly
//! allocated page at the tail and rewrites the old tail's next pointer.
//!
//! ## Durability of chain links
//!
//! A link write follows the M2a simplification of introducing no dedicated
//! WAL record type. Instead, the extension path logs a **post-image
//! `FullPageImage` record of the old tail page** (image captured after
//! `set_next_page`) and stamps the tail's `pd_lsn` with that record's LSN —
//! the same durability pattern catalog DDL already uses. On recovery the
//! default `FullPageImageRedoHandler` restores the link unconditionally, so
//! the chain is complete again before any `HeapInsert` redo runs; the heap
//! redo handlers stay stateless and never walk chains (they pin
//! `record.page_id` directly). WAL ordering makes the link consistent with
//! the new page's content for free: any durable `HeapInsert` into the new
//! page (higher LSN, same WAL stream) implies the tail's FPI is durable too;
//! if neither survived, the new page is simply unreachable and empty.
//!
//! # `t_xmin` stamping (Stage K, coding-plan Stage K row 3)
//!
//! `insert` / `update` overwrite the tuple header's fixed `t_xmin` field
//! (offset 0..8, §三) with `snapshot.current_xid` before the tuple bytes
//! reach the WAL record or the page. This is the one sanctioned exception to
//! "the AM treats tuples as opaque bytes" — it touches only the fixed header
//! field, never column data — and it closes the Stage J P2 #2 hole where a
//! caller could encode `t_xmin = 99` while writing as `current_xid = 5`,
//! making scans judge visibility by `CLOG[99]`. (`delete` / `update` already
//! stamp `t_xmax` the same way.)
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

use std::collections::{HashMap, HashSet};
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
use crate::slotted_page::{SlottedPage, HEAP_SPECIAL_SIZE};
use crate::tuple::{decode_tuple, TupleHeader, HEAP_UPDATED, TUPLE_HEADER_SIZE};

/// Largest tuple that can ever fit on a heap page (page minus special space,
/// header, and one LP).
const MAX_TUPLE_BYTES: usize = PAGE_SIZE - HEAP_SPECIAL_SIZE - PAGE_HEADER_SIZE - LINE_POINTER_SIZE;

/// Heap access method over the shared data file.
///
/// Per relation OID, `HeapAM` caches the list of pages that hold its tuples.
/// The cache is seeded lazily by walking the on-disk page chain from
/// [`RelationDesc::first_page`] (`seed_from_chain`) the first time a relation
/// is touched, then grows as inserts extend the chain — see the module docs.
pub struct HeapAM {
    buffer_pool: Arc<BufferPool>,
    wal_writer: Arc<WalWriter>,
    /// Per-relation page lists (see the struct docs).
    pages: Mutex<HashMap<Oid, Vec<PageId>>>,
    /// Serializes chain extension: without it, two threads that both find no
    /// page with room would fork the chain by linking two different pages
    /// from the same tail. Only extenders take this lock, and never while
    /// holding a page latch, so it cannot deadlock with the update path
    /// (which takes page latches but never this lock).
    extend_lock: Mutex<()>,
}

impl HeapAM {
    /// Create a heap AM bound to the engine's buffer pool and WAL writer.
    pub fn new(buffer_pool: Arc<BufferPool>, wal_writer: Arc<WalWriter>) -> Self {
        HeapAM {
            buffer_pool,
            wal_writer,
            pages: Mutex::new(HashMap::new()),
            extend_lock: Mutex::new(()),
        }
    }

    /// Allocate and initialize a relation's first heap page, tracking it as a
    /// one-page chain (`next = None`).
    ///
    /// Convenience for callers/tests that need to materialize a brand-new,
    /// empty heap before inserting. The `PageAlloc` record written by
    /// `new_page` extends the data file, so recovery can pin the page even if
    /// it was never flushed. The page's `init` is made durable with a
    /// post-image `FullPageImage` record (see [`Self::extend_chain`]): the
    /// page may have come from the freelist, where a previous tenant's
    /// content still sits on disk, and "fresh page" detection on the
    /// recovery side keys off an all-zero page — replaying the init image is
    /// what guarantees a reused page is seen as freshly initialized rather
    /// than as its previous tenant's data.
    pub fn create_heap(&self, rel_oid: Oid) -> Result<PageId> {
        let mut guard = self.buffer_pool.new_page()?;
        let page_id = guard.page_id();
        {
            let page = as_page_mut(&mut guard);
            SlottedPage::init_with_special(page, HEAP_SPECIAL_SIZE);
            self.log_page_init(page_id, page)?;
        }
        self.pages
            .lock()
            .expect("heap page map poisoned")
            .insert(rel_oid, vec![page_id]);
        Ok(page_id)
    }

    /// Append a post-image `FullPageImage` of a freshly initialized page and
    /// stamp its `pd_lsn` — the durability anchor for page initialization.
    /// Without it, a freelist-reused page whose previous tenant's bytes are
    /// still on disk would be read back as that tenant's data on recovery
    /// (redo and `seed_from_chain` both key "freshness" off page content).
    fn log_page_init(&self, page_id: PageId, page: &mut [u8; PAGE_SIZE]) -> Result<()> {
        let image = page.to_vec();
        let lsn = self
            .wal_writer
            .append(WalRecord::full_page_image(page_id, image)?)?;
        stamp_pd_lsn(page, lsn);
        Ok(())
    }

    /// Return a snapshot of the pages tracked for `rel`, seeding the cache
    /// from the on-disk chain on first touch.
    fn relation_pages(&self, rel: &RelationDesc<'_>) -> Result<Vec<PageId>> {
        if let Some(pages) = self
            .pages
            .lock()
            .expect("heap page map poisoned")
            .get(&rel.rel_oid)
        {
            return Ok(pages.clone());
        }
        let seeded = self.seed_from_chain(rel.first_page)?;
        // Another thread may have seeded (and even extended) the same
        // relation concurrently; the existing cache entry wins because it is
        // at least as fresh as anything readable from disk.
        let mut map = self.pages.lock().expect("heap page map poisoned");
        Ok(map.entry(rel.rel_oid).or_insert(seeded).clone())
    }

    /// Walk the on-disk page chain from `first_page`, collecting the
    /// relation's pages in chain order.
    ///
    /// A fresh (all-zero) page ends the walk: it is a page whose allocation
    /// (`PageAlloc`) and incoming link survived a crash but whose first
    /// `HeapInsert` did not — keeping it in the list lets a later insert
    /// initialize and reuse it instead of leaking it. A cycle or an
    /// unreadable chain pointer is catalog-level corruption and fails loudly.
    fn seed_from_chain(&self, first_page: PageId) -> Result<Vec<PageId>> {
        let mut pages = vec![first_page];
        let mut seen = HashSet::from([first_page]);
        loop {
            let current = *pages.last().expect("pages starts non-empty");
            let guard = self.buffer_pool.pin(current)?;
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
            // A fresh page has no header yet, so there is no special space to
            // read a next pointer from.
            if SlottedPage::header(page).pd_upper == 0 {
                break;
            }
            let Some(next) = SlottedPage::next_page(page)? else {
                break;
            };
            if !seen.insert(next) {
                return Err(HeapError::Corrupted(format!(
                    "page chain cycle detected at page {next} (head {first_page})"
                )));
            }
            pages.push(next);
        }
        Ok(pages)
    }

    /// Record that `page_id` now belongs to `rel_oid` (idempotent).
    fn track_page(&self, rel_oid: Oid, page_id: PageId) {
        let mut map = self.pages.lock().expect("heap page map poisoned");
        let list = map.entry(rel_oid).or_default();
        if !list.contains(&page_id) {
            list.push(page_id);
        }
    }

    /// Drop the cached page list of `rel_oid` (Stage K engine DDL).
    ///
    /// Called by `drop_table` after the relation's pages have been freed:
    /// freed page IDs can be handed out again, and a stale cache entry would
    /// route a future relation's reads/writes into pages it does not own.
    /// The on-disk chain is untouched (the pages themselves are freed by the
    /// caller); this only clears the in-memory cache. Dropping an unknown
    /// relation is a no-op.
    pub fn drop_relation(&self, rel_oid: Oid) {
        self.pages
            .lock()
            .expect("heap page map poisoned")
            .remove(&rel_oid);
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

    /// Overwrite `t_xmin` (tuple-header fixed field, offset 0..8, §三) with
    /// the writer's own XID, returning the stamped tuple bytes.
    ///
    /// This is the one place the AM breaks "tuples are opaque bytes": it
    /// touches only the fixed header field, never column data (see the
    /// module docs). Stamping happens before the WAL record is built, so the
    /// logged bytes — and therefore any tuple reconstructed by redo — carry
    /// the writer's XID, not whatever the caller encoded.
    fn stamp_xmin(tuple: &[u8], xid: TxnId) -> Result<Vec<u8>> {
        if tuple.len() < TUPLE_HEADER_SIZE {
            return Err(HeapError::InvalidArgument(format!(
                "tuple of {} bytes is shorter than the {}-byte header",
                tuple.len(),
                TUPLE_HEADER_SIZE
            )));
        }
        let mut owned = tuple.to_vec();
        owned[0..8].copy_from_slice(&xid.0.to_le_bytes());
        Ok(owned)
    }

    /// Pin a page (initialized) that has room for `needed` bytes, excluding
    /// `exclude`, extending the chain with a fresh page if none of the
    /// relation's existing pages qualify. The returned guard is held for the
    /// caller's mutation.
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
        for page_id in self.relation_pages(rel)?.into_iter().rev() {
            if page_id == exclude {
                continue;
            }
            let mut guard = self.buffer_pool.pin_mut(page_id)?;
            {
                let page = as_page_mut(&mut guard);
                SlottedPage::init_if_fresh_with_special(page, HEAP_SPECIAL_SIZE);
                if SlottedPage::free_space(page) >= needed {
                    return Ok(guard);
                }
            }
        }
        self.extend_chain(rel, needed, exclude)
    }

    /// Append a freshly allocated page to the relation's chain and return it
    /// pinned for write.
    ///
    /// Serialized by `extend_lock` (see the struct docs). After taking the
    /// lock the current tail is re-checked: a concurrent extender may have
    /// just appended a page that still has room, in which case no new page is
    /// allocated at all.
    ///
    /// The link from the old tail is made durable with a post-image
    /// `FullPageImage` record of the tail page (see the module docs'
    /// "Durability of chain links"), not with a new WAL record type.
    fn extend_chain(
        &self,
        rel: &RelationDesc<'_>,
        needed: usize,
        exclude: PageId,
    ) -> Result<PageGuardMut<'_>> {
        let _serialize = self.extend_lock.lock().expect("heap extend lock poisoned");

        let pages = self.relation_pages(rel)?;
        let tail = *pages.last().expect("seeded chain is non-empty");
        if tail != exclude {
            let mut guard = self.buffer_pool.pin_mut(tail)?;
            {
                let page = as_page_mut(&mut guard);
                SlottedPage::init_if_fresh_with_special(page, HEAP_SPECIAL_SIZE);
                if SlottedPage::free_space(page) >= needed {
                    // A concurrent extender already added room.
                    return Ok(guard);
                }
            }
        }

        // Allocate and initialize the new tail. A valid tuple always fits on
        // a freshly initialized page (validate_tuple_len bounds it), so the
        // caller can add_tuple unconditionally. The init is WAL-logged via
        // `log_page_init` so a freelist-reused page recovers as freshly
        // initialized, not as its previous tenant's bytes.
        let mut new_guard = self.buffer_pool.new_page()?;
        let new_page_id = new_guard.page_id();
        {
            let page = as_page_mut(&mut new_guard);
            SlottedPage::init_with_special(page, HEAP_SPECIAL_SIZE);
            self.log_page_init(new_page_id, page)?;
        }

        // Link the old tail to the new page, then log the tail's post-image
        // FPI and stamp its pd_lsn — all while holding the tail's write
        // latch, so no flush can slip between the link write and the pd_lsn
        // stamp (WAL-before-data for the link).
        {
            let mut tail_guard = self.buffer_pool.pin_mut(tail)?;
            let page = as_page_mut(&mut tail_guard);
            SlottedPage::init_if_fresh_with_special(page, HEAP_SPECIAL_SIZE);
            SlottedPage::set_next_page(page, Some(new_page_id));
            let image = page.to_vec();
            let lsn = self
                .wal_writer
                .append(WalRecord::full_page_image(tail, image)?)?;
            stamp_pd_lsn(page, lsn);
        }

        self.track_page(rel.rel_oid, new_page_id);
        Ok(new_guard)
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

    /// Read the tuple header at `tid`'s slot, or [`HeapError::TupleNotFound`]
    /// if the tuple is not live.
    ///
    /// Liveness is LP `Normal` **and** the tuple is not *effectively*
    /// deleted: `t_xmax == INVALID`, or the stamping deleter ABORTED (its
    /// delete never took effect, so the tuple may be deleted/updated again —
    /// without consulting the CLOG such a tuple would stay visible yet be
    /// permanently unmodifiable). A tuple whose deleter COMMITTED is dead
    /// (`TupleNotFound`); one whose deleter is still IN_PROGRESS is also
    /// rejected — overwriting its stamp would resurrect the row if the
    /// original deleter commits and the overwriter aborts (M2a has no
    /// wait-for-xmax machinery; callers may retry once it resolves).
    fn live_tuple_header(
        page: &[u8; PAGE_SIZE],
        tid: Tid,
        clog: &dyn ClogAccessor,
    ) -> Result<TupleHeader> {
        let lp = SlottedPage::line_pointer(page, tid.slot_id)?;
        if lp.flags() != LpFlags::Normal {
            return Err(HeapError::TupleNotFound(tid));
        }
        let off = lp.off() as usize;
        let header = TupleHeader::read_from(&page[off..off + TUPLE_HEADER_SIZE])?;
        if header.t_xmax != TxnId::INVALID && clog.get_state(header.t_xmax) != TxnState::Aborted {
            return Err(HeapError::TupleNotFound(tid));
        }
        Ok(header)
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
        // Stamp t_xmin with the writer's own XID before the bytes reach the
        // WAL record or the page (see the module docs).
        let tuple = Self::stamp_xmin(tuple, snapshot.current_xid)?;

        let needed = tuple.len() + LINE_POINTER_SIZE;
        let mut guard = self.acquire_page_with_room(&rel, needed, PageId::INVALID)?;
        let page_id = guard.page_id();
        let page = as_page_mut(&mut guard);

        // add_tuple always appends on a heap page (no Unused slots to recycle),
        // so the slot is known before the mutation — build the WAL record first.
        let slot = SlottedPage::slot_count(page) as u16;
        let rec = WalRecord::heap_insert(page_id, slot, tuple.clone(), snapshot.current_xid)?;
        let lsn = self.wal_writer.append(rec)?;
        let actual = SlottedPage::add_tuple(page, &tuple)?;
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
        for page_id in self.relation_pages(&ctx.rel)? {
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
        // Liveness is CLOG-aware (an aborted deleter's stamp does not count
        // as a delete), not just `t_xmax == INVALID`.
        Self::live_tuple_header(page, tid, ctx.clog)?;

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
            clog,
        } = ctx;
        Self::validate_tuple_len(new_tuple)?;
        let xmax = snapshot.current_xid;
        // Stamp the new version's t_xmin with the writer's own XID (module
        // docs); t_xmax of the old version is stamped by `stamp_deleted`.
        let new_tuple = Self::stamp_xmin(new_tuple, xmax)?;
        let needed = new_tuple.len() + LINE_POINTER_SIZE;

        // Fast path: pin the old page, verify the target tuple is live, and
        // check whether the new version fits alongside it (single latch,
        // single page). Stamping the old tuple does not change slot_count, so
        // the new slot is `slot_count` and add_tuple appends there.
        let mut old_guard = self.buffer_pool.pin_mut(old_tid.page_id)?;
        let old_has_room = {
            let old_page = as_page_mut(&mut old_guard);
            // Live = LP Normal AND not effectively deleted (CLOG-aware: an
            // aborted deleter's stamp does not count).
            Self::live_tuple_header(old_page, old_tid, clog)?;
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
            let rec = WalRecord::heap_update(old_tid, new_tid, xmax, new_tuple.clone(), xmax)?;
            let lsn = self.wal_writer.append(rec)?;
            Self::stamp_deleted(old_page, old_tid, xmax, true)?;
            let actual = SlottedPage::add_tuple(old_page, &new_tuple)?;
            debug_assert_eq!(actual, new_slot);
            stamp_pd_lsn(old_page, lsn);
            if let Some(out) = out_tid {
                *out = new_tid;
            }
            return Ok(());
        }

        // Cross-page: the old page has no room. Drop its latch BEFORE
        // acquiring the new page: chain extension pins the chain tail, and
        // holding the old page's latch across that would invert the lock
        // order (extend path: extend_lock → tail latch) and could deadlock
        // when the old page IS the tail. The old tuple's liveness is
        // re-verified below before any heap WAL record is written; a page
        // allocated on behalf of an update that loses that race is simply
        // left empty (still tracked in the page cache, reused by the next
        // insert) — never a poison WAL record.
        drop(old_guard);
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

        // Re-pin the old page and re-verify the target is still live before
        // writing WAL (a rejected update must leave no HeapUpdate record
        // behind for recovery to choke on — same discipline as delete).
        // Liveness is CLOG-aware: a tuple whose committed deleter (or
        // still-in-progress deleter) stamped it while this update dropped the
        // latch is rejected, not overwritten; an aborted deleter's stamp does
        // not count.
        let mut old_guard = self.buffer_pool.pin_mut(old_tid.page_id)?;
        {
            let old_page = as_page_mut(&mut old_guard);
            Self::live_tuple_header(old_page, old_tid, clog)?;
        }

        let rec = WalRecord::heap_update(old_tid, new_tid, xmax, new_tuple.clone(), xmax)?;
        let lsn = self.wal_writer.append(rec)?;

        {
            let old_page = as_page_mut(&mut old_guard);
            Self::stamp_deleted(old_page, old_tid, xmax, true)?;
            stamp_pd_lsn(old_page, lsn);
        }
        {
            let new_page = as_page_mut(&mut new_guard);
            let actual = SlottedPage::add_tuple(new_page, &new_tuple)?;
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
    /// Scan `rel` for dead tuples.
    ///
    /// # InProgress vs Aborted (建档 note)
    ///
    /// The "InProgress ≡ Aborted for visibility" equivalence that recovery
    /// relies on (pg-storage `analysis` module docs) holds ONLY for
    /// visibility, not for reclamation: a tuple inserted by a crashed
    /// transaction has `t_xmin` whose CLOG entry reads `InProgress` (no
    /// terminal record exists), so case 1 below does NOT collect it. Such
    /// orphan tuples are reclaimed only once the crashed XIDs are
    /// explicitly stamped ABORTED — recovery-end ATT marking is M2c work
    /// (and vacuum/autovacuum M3); until then they are dead weight but
    /// never visible.
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
        let page_ids = self.relation_pages(&rel)?;

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
