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
//!
//! # Row-lock `t_xmax` protocol (M2c Stage P, tech-selection §9.1)
//!
//! Write-write arbitration on a row lives in its `t_xmax`: any non-INVALID
//! `t_xmax` — a real delete/update stamp OR a [`HEAP_XMAX_LOCK_ONLY`] stamp
//! (`SELECT ... FOR UPDATE`) — means "row locked". A writer reaching a row
//! runs the 5-step protocol ([`HeapAM::row_lock_gate`] + the restart loops
//! in `delete` / `update` / [`HeapAM::lock_tuple`]):
//!
//! 1. under the page write latch, read `t_xmax`;
//! 2. `t_xmax == INVALID` or `== self` → stamp immediately (the latch
//!    serializes check and stamp, so the pair IS the "CAS" of §9.1);
//! 3. `t_xmax` of a COMMITTED real deleter →
//!    [`HeapError::TupleConcurrentlyUpdated`] (the addressed version is
//!    dead; distinct from "row does not exist"). A committed LOCK_ONLY
//!    stamp is NOT a delete: the row stays modifiable and the stamp is
//!    simply overwritten;
//! 4. `t_xmax` of an ABORTED stamper → overwrite, same as step 2;
//! 5. `t_xmax` of a still-active OTHER transaction → register the wait edge
//!    in the `TxnManager`'s `row_wait_registry` WHILE STILL HOLDING THE
//!    LATCH (step 5a), then release the latch (5b), block in
//!    `TxnManager::wait_for` (5c) until the holder's commit/abort broadcast
//!    (5d), and restart from step 1 (5e).
//!
//! Registration strictly precedes latch release, so the wakeup can never be
//! missed: the holder's `end_txn` broadcast is serialized against the
//! registry by its mutex, and the latch serializes the stamper against any
//! state change of `t_xmax`.
//!
//! ## Backward compatibility (no waiter installed)
//!
//! The wait capability arrives via [`HeapAM::set_row_waiter`] (the engine
//! installs the `TxnManager` at open). A `HeapAM` WITHOUT a waiter — every
//! pre-Stage-P construction site — keeps the old "first-writer-wins +
//! second-writer-errors" behavior: any non-INVALID, non-ABORTED `t_xmax`
//! (committed OR in-progress) is rejected with [`HeapError::TupleNotFound`]
//! instead of waiting, and no `TupleConcurrentlyUpdated` is produced.
//!
//! ## Lock-only stamps and visibility
//!
//! A [`HEAP_XMAX_LOCK_ONLY`] stamp is a lock, not a delete: scan/visibility
//! paths mask it to INVALID before judging ([`visibility_xmax`]), so a
//! locked row reads as live for everyone. Lock-only stamps are NOT
//! WAL-logged (PostgreSQL does not log row locks either): they are
//! transient concurrency markers whose meaning ends with the stamper's
//! transaction, and a stamp that survives a crash reads as an in-progress
//! or aborted XID — never hiding the row.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use pg_storage::buffer_pool::{BufferPool, PageGuardMut};
use pg_storage::clog::{ClogAccessor, TxnState};
use pg_storage::page::{page_pd_lsn, set_page_pd_lsn, PAGE_HEADER_SIZE};
use pg_storage::recovery::RedoHandler;
use pg_storage::types::{Lsn, Oid, PageId, Tid, TxnId, PAGE_SIZE};
use pg_storage::wal::record::WalRecord;
use pg_storage::wal::WalWriter;

use pg_txn::{is_visible, RowWaiter, Snapshot};

use crate::access_method::{
    AccessMethod, DeleteContext, InsertContext, RelationDesc, ScanContext, UpdatableAM,
    UpdateContext, Vacuumable,
};
use crate::error::{HeapError, Result};
use crate::line_pointer::{LpFlags, LINE_POINTER_SIZE};
use crate::redo::{HeapDeleteHandler, HeapInsertHandler, HeapUpdateHandler};
use crate::slotted_page::{SlottedPage, HEAP_SPECIAL_SIZE};
use crate::tuple::{
    decode_tuple, TupleHeader, HEAP_UPDATED, HEAP_XMAX_LOCK_ONLY, TUPLE_HEADER_SIZE,
};

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
    /// Row-lock wait capability for the §9.1 5-step protocol (M2c Stage P),
    /// installed by the engine via [`Self::set_row_waiter`]. `None` keeps
    /// the pre-Stage-P "second-writer-errors" behavior — see the module
    /// docs' backward-compatibility section.
    row_waiter: Option<Arc<dyn RowWaiter>>,
}

impl HeapAM {
    /// Create a heap AM bound to the engine's buffer pool and WAL writer.
    pub fn new(buffer_pool: Arc<BufferPool>, wal_writer: Arc<WalWriter>) -> Self {
        HeapAM {
            buffer_pool,
            wal_writer,
            pages: Mutex::new(HashMap::new()),
            extend_lock: Mutex::new(()),
            row_waiter: None,
        }
    }

    /// Install the row-lock wait capability (M2c Stage P). Called once by
    /// the engine at open time, before the AM is shared: the field is a
    /// plain `Option`, so installing requires `&mut self` and cannot race
    /// concurrent use.
    pub fn set_row_waiter(&mut self, waiter: Arc<dyn RowWaiter>) {
        self.row_waiter = Some(waiter);
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

    /// Stamp `t_xmax`, `t_cid`, and optionally the `HEAP_UPDATED` infomask bit
    /// onto the live tuple at `tid`'s slot, in place. TID stability is
    /// preserved: the line pointer stays `Normal`.
    fn stamp_deleted(
        page: &mut [u8; PAGE_SIZE],
        tid: Tid,
        xmax: TxnId,
        curcid: u32,
        updated: bool,
    ) -> Result<()> {
        let lp = SlottedPage::line_pointer(page, tid.slot_id)?;
        if lp.flags() != LpFlags::Normal {
            return Err(HeapError::TupleNotFound(tid));
        }
        let off = lp.off() as usize;
        let mut header = TupleHeader::read_from(&page[off..off + TUPLE_HEADER_SIZE])?;
        header.t_xmax = xmax;
        header.t_cid = curcid;
        if updated {
            header.t_infomask |= HEAP_UPDATED;
        }
        // A real delete/update supersedes any lock-only stamp on the row
        // (e.g. `SELECT ... FOR UPDATE` followed by DELETE in the same
        // transaction): leaving LOCK_ONLY set would mask the delete from
        // visibility checks and resurrect the row for scans.
        header.t_infomask &= !HEAP_XMAX_LOCK_ONLY;
        header.write_to(&mut page[off..off + TUPLE_HEADER_SIZE]);
        Ok(())
    }

    /// §9.1 steps 1–5a of the row-lock protocol: read the tuple header at
    /// `tid`'s slot (under the page write latch the caller holds) and decide
    /// whether the caller may stamp the tuple.
    ///
    /// `self_xid` is the writer's own XID (`snapshot.current_xid`) — the
    /// row-lock identity. A `t_xmax` naming `self_xid` is a self-conflict:
    /// the caller already locked/deleted/updated this row version inside its
    /// own transaction and simply proceeds (never waits on itself).
    ///
    /// `for_lock` marks the lock-only acquisition path
    /// ([`Self::lock_tuple`]): re-stamping a row whose existing stamp is my
    /// own REAL delete/update (not [`HEAP_XMAX_LOCK_ONLY`]) would re-add the
    /// lock-only bit on top of a delete stamp, and the visibility mask
    /// would then resurrect the row — so that combination is rejected and
    /// only idempotent re-locking of my own LOCK_ONLY stamp proceeds.
    /// Delete/update pass `false` (overwriting their own stamp is the
    /// normal same-transaction re-write).
    ///
    /// When the verdict is [`RowLockGate::Wait`] the wait edge
    /// (`self_xid → t_xmax`) is registered BEFORE this function returns —
    /// i.e. still under the latch — so the caller releasing the latch and
    /// sleeping cannot miss the blocker's commit/abort broadcast (the
    /// step-5a-before-5b ordering, see the module docs).
    ///
    /// # Errors
    ///
    /// - [`HeapError::TupleNotFound`]: the slot does not hold a live
    ///   (`Normal`) tuple. ALSO the legacy no-waiter behavior for a
    ///   committed or in-progress `t_xmax` (module docs, backward
    ///   compatibility).
    /// - [`HeapError::TupleConcurrentlyUpdated`] (§9.1 step 3): `t_xmax` is
    ///   a REAL delete/update stamp (not [`HEAP_XMAX_LOCK_ONLY`]) whose
    ///   transaction COMMITTED — the addressed row version is dead. A
    ///   committed LOCK_ONLY stamp is not a delete: the row stays
    ///   modifiable and the stamp is overwritten (Proceed).
    /// - [`HeapError::InvalidArgument`] (`for_lock` only): the row already
    ///   carries MY real delete/update stamp (see above).
    fn row_lock_gate(
        &self,
        page: &[u8; PAGE_SIZE],
        tid: Tid,
        self_xid: TxnId,
        clog: &dyn ClogAccessor,
        for_lock: bool,
    ) -> Result<RowLockGate> {
        let lp = SlottedPage::line_pointer(page, tid.slot_id)?;
        if lp.flags() != LpFlags::Normal {
            return Err(HeapError::TupleNotFound(tid));
        }
        let off = lp.off() as usize;
        let header = TupleHeader::read_from(&page[off..off + TUPLE_HEADER_SIZE])?;
        let xmax = header.t_xmax;
        // Step 2 (no stamp yet) / self-conflict (my own stamp).
        if xmax == TxnId::INVALID || xmax == self_xid {
            if for_lock && xmax == self_xid && header.t_infomask & HEAP_XMAX_LOCK_ONLY == 0 {
                return Err(HeapError::InvalidArgument(format!(
                    "cannot lock {tid:?}: row version already deleted or updated by this transaction"
                )));
            }
            return Ok(RowLockGate::Proceed);
        }
        let lock_only = header.t_infomask & HEAP_XMAX_LOCK_ONLY != 0;
        let mut state = clog.get_state(xmax);
        if matches!(state, TxnState::InProgress | TxnState::SubCommitted) {
            // Step 5a: the holder LOOKS active. `SubCommitted` (M3-reserved,
            // never produced in M2) folds in here: a sub-committed stamper's
            // parent may still abort, so it is "not terminally committed",
            // matching the visibility oracle's `!= Committed` treatment.
            match &self.row_waiter {
                Some(waiter) => {
                    if waiter.is_active(xmax) {
                        // Genuinely active holder: register the wait edge
                        // UNDER THE LATCH; the caller releases latches and
                        // blocks (steps 5b/5c).
                        waiter.register_row_wait(self_xid, xmax);
                        return Ok(RowLockGate::Wait(xmax));
                    }
                    // Not active despite the InProgress CLOG read. Two
                    // cases:
                    //
                    // - The stamper ENDED between our CLOG read and the
                    //   active-set check (normal race): `end_txn` flips the
                    //   CLOG bit BEFORE removing the XID from the active
                    //   set, so observing not-active orders us after the
                    //   terminal write — re-reading the CLOG now yields the
                    //   terminal state, which the match below handles.
                    // - The stamper CRASHED (post-recovery; recovery-end
                    //   ATT abort marking is still open, §11.3): the CLOG
                    //   re-read still says InProgress. WAL replay rebuilt
                    //   every durable commit's bit, so this means "never
                    //   committed" — treat the stamp as aborted (Proceed).
                    //   Waiting would spin forever on a transaction that
                    //   can never end.
                    state = clog.get_state(xmax);
                }
                None => return Err(HeapError::TupleNotFound(tid)), // legacy mode
            }
        }
        match state {
            // Step 4: the stamp never took effect (or its stamper crashed);
            // overwrite it. A terminal LOCK_ONLY stamp (committed or
            // aborted) lands in the Proceed arms too — a lock is not a
            // delete, so the row stays modifiable.
            TxnState::Aborted => Ok(RowLockGate::Proceed),
            // InProgress/SubCommitted here is only reachable via the
            // crashed-stamper re-read above (a live holder took the `Wait`
            // early return; legacy mode returned already).
            TxnState::InProgress | TxnState::SubCommitted => Ok(RowLockGate::Proceed),
            TxnState::Committed if lock_only => Ok(RowLockGate::Proceed),
            // Step 3: a committed real delete/update owns this version.
            TxnState::Committed => match &self.row_waiter {
                Some(_) => Err(HeapError::TupleConcurrentlyUpdated(tid)),
                None => Err(HeapError::TupleNotFound(tid)), // legacy mode
            },
        }
    }

    /// §9.1 steps 5b–5c: block until `blocking_xid` ends. The caller must
    /// have dropped every page latch already; the wait edge was registered
    /// by [`Self::row_lock_gate`] while the latch was still held.
    ///
    /// A [`TxnError::DeadlockVictim`] interruption (M2c Stage R) maps to
    /// [`HeapError::DeadlockVictim`] so the SQL layer can tell a
    /// detector-chosen abort from an internal wait failure.
    fn wait_row_lock(&self, self_xid: TxnId, blocking_xid: TxnId) -> Result<()> {
        let waiter = self
            .row_waiter
            .as_ref()
            .expect("row_lock_gate only returns Wait with a waiter installed");
        waiter
            .wait_for(self_xid, blocking_xid)
            .map_err(|e| {
                // Unreachable through the gate (it never returns
                // `Wait(self_xid)`), but a failed wait must not leak the
                // registered edge — Stage R's deadlock detector reads the
                // registry as the wait-for graph. (Idempotent: `wait_for`
                // already cleared the edge on its own error paths.)
                waiter.unregister_row_wait(self_xid);
                match e {
                    pg_txn::TxnError::DeadlockVictim(_) => HeapError::DeadlockVictim,
                    other => {
                        HeapError::InvalidArgument(format!("row-lock wait failed: {other}"))
                    }
                }
            })
    }

    /// Acquire the §9.1 row lock on the tuple at `tid` WITHOUT deleting it
    /// (M2c Stage P: `SELECT ... FOR UPDATE`): stamps
    /// `t_xmax = snapshot.current_xid` with [`HEAP_XMAX_LOCK_ONLY`] set and
    /// `t_cid = snapshot.curcid`.
    ///
    /// Same 5-step protocol as delete/update: an INVALID/self/terminal
    /// stamp is (re)acquired immediately under the page write latch; a
    /// stamp by a still-active OTHER transaction registers the wait edge
    /// under the latch, releases it, blocks in `wait_for`, and restarts.
    ///
    /// The lock-only stamp is NOT WAL-logged (see the module docs): it is a
    /// transient concurrency marker, not a visibility fact. The lock is held
    /// until the stamper's transaction ends — which is exactly what the next
    /// locker's gate consults via the CLOG/active set.
    ///
    /// # Errors
    ///
    /// [`HeapError::TupleConcurrentlyUpdated`] if the row version was
    /// deleted or updated by a transaction that has since committed; in
    /// legacy no-waiter mode that condition (and any in-progress holder) is
    /// [`HeapError::TupleNotFound`] instead — see [`Self::row_lock_gate`].
    pub fn lock_tuple(
        &self,
        tid: Tid,
        snapshot: &Snapshot,
        clog: &dyn ClogAccessor,
    ) -> Result<()> {
        let self_xid = snapshot.current_xid;
        debug_assert!(
            self_xid != TxnId::INVALID,
            "lock_tuple with INVALID current_xid would stamp a no-op lock"
        );
        // §9.1 restart loop (steps 5d→1): identical shape to delete/update.
        // Every wait implies the counterparty ended (progress), so the loop
        // converges in practice; the counter turns a hypothetical livelock
        // into a debug-build panic instead of a silent spin (P2-2).
        let mut restarts = 0u32;
        loop {
            restarts += 1;
            debug_assert!(
                restarts < 10_000,
                "lock_tuple restart loop failed to converge (xid {self_xid})"
            );
            let mut guard = self.buffer_pool.pin_mut(tid.page_id)?;
            let gate = {
                let page = as_page_mut(&mut guard);
                self.row_lock_gate(page, tid, self_xid, clog, true)?
            };
            match gate {
                RowLockGate::Proceed => {
                    let page = as_page_mut(&mut guard);
                    Self::stamp_lock_only(page, tid, self_xid, snapshot.curcid)?;
                    return Ok(());
                }
                RowLockGate::Wait(blocking) => {
                    // Step 5b: release the latch BEFORE sleeping (the edge
                    // is already registered, so no wakeup can be missed);
                    // 5c: block; the loop restarts at step 1.
                    drop(guard);
                    self.wait_row_lock(self_xid, blocking)?;
                }
            }
        }
    }

    /// Stamp the §9.1 lock-only mark (`t_xmax` + [`HEAP_XMAX_LOCK_ONLY`] +
    /// `t_cid`) in place. The tuple stays visible to every snapshot — a
    /// lock is not a delete — but the row-lock protocol treats the
    /// non-INVALID `t_xmax` as "row locked" until the stamper ends.
    ///
    /// The `t_cid` overwrite is LOSSY: if the same statement first inserted
    /// this row and then locked it (self-insert at `t_cid == curcid`,
    /// re-locked at the same curcid), the row would read as written-by-
    /// current-command and become invisible to the statement's own re-scan.
    /// Unreachable today: the executor never locks a row it wrote in the
    /// same statement (FOR UPDATE scans see only earlier-command rows), so
    /// the overwritten `t_cid` is always from a completed command.
    ///
    /// TODO: revisit when subtransactions or EvalPlanQual land — both make
    /// same-command lock-after-write reachable and need a non-lossy
    /// cmin/cmax representation (see the Stage O trade-off entry in
    /// docs/stage_spec.md).
    fn stamp_lock_only(
        page: &mut [u8; PAGE_SIZE],
        tid: Tid,
        locker: TxnId,
        curcid: u32,
    ) -> Result<()> {
        let lp = SlottedPage::line_pointer(page, tid.slot_id)?;
        if lp.flags() != LpFlags::Normal {
            return Err(HeapError::TupleNotFound(tid));
        }
        let off = lp.off() as usize;
        let mut header = TupleHeader::read_from(&page[off..off + TUPLE_HEADER_SIZE])?;
        header.t_xmax = locker;
        header.t_cid = curcid;
        header.t_infomask |= HEAP_XMAX_LOCK_ONLY;
        header.write_to(&mut page[off..off + TUPLE_HEADER_SIZE]);
        Ok(())
    }
}

/// The §9.1 gate's verdict for one tuple (see [`HeapAM::row_lock_gate`]).
enum RowLockGate {
    /// The caller may stamp the tuple now, still under the page latch.
    Proceed,
    /// `t_xmax` names a still-active OTHER transaction; the wait edge is
    /// registered. The caller drops every latch, blocks in `wait_for`, and
    /// restarts the protocol from step 1.
    Wait(TxnId),
}

/// The `t_xmax` a visibility judgment should see: a [`HEAP_XMAX_LOCK_ONLY`]
/// stamp is a row lock, NOT a delete, so it is masked to INVALID — a locked
/// row stays live for everyone, subject to the normal `t_xmin` rules.
/// Real delete/update stamps pass through unchanged.
fn visibility_xmax(header: &TupleHeader) -> TxnId {
    if header.t_infomask & HEAP_XMAX_LOCK_ONLY != 0 {
        TxnId::INVALID
    } else {
        header.t_xmax
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
                // A HEAP_XMAX_LOCK_ONLY stamp is a row lock, not a delete:
                // mask it off so a locked row stays visible (§9.1, M2c
                // Stage P).
                if is_visible(
                    header.t_xmin,
                    visibility_xmax(&header),
                    header.t_cid,
                    ctx.snapshot,
                    clog,
                ) {
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

        // §9.1 restart loop: each iteration re-pins the page and re-runs the
        // gate from step 1; only a `Proceed` verdict falls through to the
        // WAL + stamp, still under the latch (check+stamp is the protocol's
        // "CAS"). Every wait implies the counterparty ended (progress), so
        // the loop converges in practice; the counter turns a hypothetical
        // livelock into a debug-build panic instead of a silent spin (P2-2).
        let mut restarts = 0u32;
        loop {
            restarts += 1;
            debug_assert!(
                restarts < 10_000,
                "delete restart loop failed to converge (xid {xmax})"
            );
            let mut guard = self.buffer_pool.pin_mut(tid.page_id)?;
            let gate = {
                let page = as_page_mut(&mut guard);
                self.row_lock_gate(page, tid, xmax, ctx.clog, false)?
            };
            if let RowLockGate::Wait(blocking) = gate {
                // Step 5b/5c: release the latch BEFORE sleeping (the edge
                // is already registered, so the wakeup cannot be missed),
                // then block; the loop restarts at step 1.
                drop(guard);
                self.wait_row_lock(xmax, blocking)?;
                continue;
            }

            let page = as_page_mut(&mut guard);
            // Validate-then-WAL discipline is unchanged (the gate ran first):
            // a rejected delete leaves no HeapDelete record behind for
            // recovery to choke on.
            let rec = WalRecord::heap_delete(tid, xmax, xmax)?;
            let lsn = self.wal_writer.append(rec)?;
            Self::stamp_deleted(page, tid, xmax, ctx.snapshot.curcid, false)?;
            stamp_pd_lsn(page, lsn);
            return Ok(());
        }
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

        // §9.1 restart loop: the gate (steps 1–5a) runs under the old page's
        // write latch; on `Wait` EVERY latch is dropped before sleeping, and
        // the whole path — including the room check and any chain extension
        // — restarts from step 1 (the tuple's state may have changed
        // arbitrarily while we slept). Convergence argument and the debug
        // counter: same as delete/lock_tuple (P2-2).
        let mut restarts = 0u32;
        loop {
            restarts += 1;
            debug_assert!(
                restarts < 10_000,
                "update restart loop failed to converge (xid {xmax})"
            );
            // Fast path: pin the old page, run the gate, and check whether
            // the new version fits alongside it (single latch, single page).
            // Stamping the old tuple does not change slot_count, so the new
            // slot is `slot_count` and add_tuple appends there.
            let mut old_guard = self.buffer_pool.pin_mut(old_tid.page_id)?;
            let gate = {
                let old_page = as_page_mut(&mut old_guard);
                self.row_lock_gate(old_page, old_tid, xmax, clog, false)?
            };
            if let RowLockGate::Wait(blocking) = gate {
                drop(old_guard);
                self.wait_row_lock(xmax, blocking)?;
                continue;
            }
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
                let rec = WalRecord::heap_update(old_tid, new_tid, xmax, new_tuple.clone(), xmax)?;
                let lsn = self.wal_writer.append(rec)?;
                Self::stamp_deleted(old_page, old_tid, xmax, snapshot.curcid, true)?;
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
            // when the old page IS the tail. The gate is re-run below after
            // re-pinning, before any heap WAL record is written; a page
            // allocated on behalf of an update that loses that race is simply
            // left empty (still tracked in the page cache, reused by the next
            // insert) — never a poison WAL record.
            drop(old_guard);
            let new_guard = self.acquire_page_with_room(&rel, needed, old_tid.page_id)?;
            let new_page_id = new_guard.page_id();

            // Two-latch acquisition follows a GLOBAL order — smaller PageId
            // first (M2c Stage P review): two concurrent cross-page updates
            // can pick each other's old page as their new page, and an
            // unordered hold-and-wait is an AB/BA deadlock on buffer-pool
            // latches, which have no timeout and are invisible to Stage R's
            // (lock-manager-based) deadlock detector. When the old page is
            // the smaller one, the new guard is dropped and both pages are
            // re-latched in order; the new page's room is re-checked because
            // a filler may have taken it in between (restart the whole
            // protocol if so — the fast path above will re-evaluate).
            let (mut old_guard, mut new_guard) = if old_tid.page_id < new_page_id {
                drop(new_guard);
                let old_guard = self.buffer_pool.pin_mut(old_tid.page_id)?;
                let new_guard = self.buffer_pool.pin_mut(new_page_id)?;
                let new_has_room = {
                    let new_page: &[u8; PAGE_SIZE] =
                        new_guard.page().try_into().expect("frame is PAGE_SIZE");
                    SlottedPage::free_space(new_page) >= needed
                };
                if !new_has_room {
                    drop(new_guard);
                    drop(old_guard);
                    continue;
                }
                (old_guard, new_guard)
            } else {
                (self.buffer_pool.pin_mut(old_tid.page_id)?, new_guard)
            };

            // Re-run the gate under the old page's latch before writing WAL
            // (a rejected update must leave no HeapUpdate record behind for
            // recovery to choke on — same discipline as delete). The gate is
            // CLOG-aware: a tuple whose committed deleter stamped it while
            // this update dropped the latch is rejected, not overwritten; an
            // in-progress holder sends us to sleep; an aborted stamp does not
            // count.
            let gate = {
                let old_page = as_page_mut(&mut old_guard);
                self.row_lock_gate(old_page, old_tid, xmax, clog, false)?
            };
            if let RowLockGate::Wait(blocking) = gate {
                // Sleep holding NO latch: drop the new page's guard too — a
                // blocked waiter must never hold a write latch.
                drop(old_guard);
                drop(new_guard);
                self.wait_row_lock(xmax, blocking)?;
                continue;
            }

            // The new slot is computed only now, under the final latching:
            // in the re-ordered acquisition above the new page may have been
            // dropped and re-pinned, so any earlier slot prediction is stale.
            let new_slot = {
                let new_page = as_page_mut(&mut new_guard);
                SlottedPage::slot_count(new_page) as u16
            };
            let new_tid = Tid {
                page_id: new_page_id,
                slot_id: new_slot,
            };

            let rec = WalRecord::heap_update(old_tid, new_tid, xmax, new_tuple.clone(), xmax)?;
            let lsn = self.wal_writer.append(rec)?;

            {
                let old_page = as_page_mut(&mut old_guard);
                Self::stamp_deleted(old_page, old_tid, xmax, snapshot.curcid, true)?;
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
            return Ok(());
        }
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
                // A HEAP_XMAX_LOCK_ONLY stamp is a row lock, not a delete:
                // the tuple is never dead because of it (§9.1, M2c Stage P).
                if header.t_infomask & HEAP_XMAX_LOCK_ONLY == 0
                    && xmax != TxnId::INVALID
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
