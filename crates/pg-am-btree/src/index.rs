//! Single-threaded B+Tree core: descent, lookup, range scan, insert with
//! pessimistic splits, physical delete, and the 3-step split WAL protocol
//! (tech-selection §13.2/§13.3).
//!
//! # Tree organization
//!
//! A relation's `first_page` is the **meta page**: a slotted page whose
//! tuples are 10-byte [`page::encode_meta_record`] records
//! `(root_page_id, tree_level)`. The *last* record is authoritative; a root
//! promotion appends a new record (WAL-logged as `BTreeInsert`, so redo
//! rebuilds it under the usual `pd_lsn` guard). The root starts as a single
//! leaf (`LEAF | ROOT`) allocated right after the meta page.
//!
//! # Descent and Blink right hops (§13.2)
//!
//! Internal entries are `(low_key, child_page_id)` in sorted order; the
//! descent picks the last entry with `key <= probe` (entry 0 when the probe
//! is smaller than every key — the leftmost child covers `-infinity`).
//! Latch coupling is a single-threaded skeleton: the child guard is taken
//! before the parent guard drops.
//!
//! A split whose Commit record was lost (crash between Copy and Commit)
//! leaves the right sibling without a parent downlink. Reads stay correct
//! via Blink right hops: before descending or searching, if the probe is
//! `>=` the right sibling's first key, the descent moves right (the sibling
//! chain is the authority on which page owns a key range). This makes
//! recovered incomplete splits fully readable without repair; finishing
//! them (`BTreeSplitCLR`, undo phase) is M2c work.
//!
//! # Split: 3-step WAL (§13.3)
//!
//! The online path emits, in order, and applies each step under the affected
//! pages' write latches (WAL-before-data):
//!
//! 1. [`BTreeIndex::split_prepare`] — allocate the right sibling, emit
//!    `BTreeSplitPrepare`, link `left.next = right`, mark left
//!    `SPLIT_INCOMPLETE`, initialize the right page header.
//! 2. [`BTreeIndex::split_copy`] — emit `BTreeSplitCopy`
//!    (`copy_start_slot` + `left_page_pre_lsn` anchor), move
//!    `[copy_start_slot, slot_count)` to the right page, truncate the left LP
//!    array. The right page is then flushed **before** the left guard is
//!    released, so the left page's post-copy image can never reach disk
//!    without the right page's (redo recomputes the moved entries from the
//!    left page; that contract would break otherwise).
//! 3. [`BTreeIndex::split_commit`] — emit `BTreeSplitCommit`, insert the
//!    downlink `(separator_key, right_page)` into the parent (splitting the
//!    parent recursively first if it has no room; a root split allocates a
//!    new root and appends a meta record), clear `SPLIT_INCOMPLETE` (and
//!    `ROOT`, for root splits) on the left page.
//!
//! The downlink insert is logged **only** by the Commit record — never as a
//! separate `BTreeInsert` — so redo cannot apply it twice.
//!
//! # Delete
//!
//! Physical removal of the exact `(key, tid)` entry (`BTreeDelete` records
//! the slot; redo performs the same deterministic transformation). M2b has
//! no page merge (§13: deferred).

use std::cmp::Ordering;
use std::sync::Arc;

use pg_am_heap::slotted_page::SlottedPage;
use pg_am_heap::tuple::ColumnType;
use pg_storage::buffer_pool::{BufferPool, PageGuardMut};
use pg_storage::page::{page_pd_lsn, set_page_pd_lsn};
use pg_storage::types::{Lsn, Oid, PageId, Tid, PAGE_SIZE};
use pg_storage::wal::record::WalRecord;
use pg_storage::wal::WalWriter;

use crate::error::{BTreeError, Result};
use crate::key::{is_supported_key_type, MAX_INDEX_KEY_BYTES};
use crate::page::{self, BtreePage, BTREE_FLAG_LEAF, BTREE_FLAG_ROOT, BTREE_FLAG_SPLIT_INCOMPLETE};

/// Bound on sibling hops per descent / chain walk; a longer chain means a
/// corrupted `btpo_next` cycle (or a pathological run of incomplete splits),
/// and must hard-fail rather than loop forever.
const MAX_CHAIN_HOPS: usize = 1 << 16;

/// State carried between the three split steps (§13.3).
///
/// The steps are separate entry points so crash tests can drive a split
/// one step at a time and abandon the engine mid-protocol; the online
/// insert path runs all three back to back.
#[derive(Debug, Clone)]
pub struct SplitState {
    /// The overflowing original page.
    pub left: PageId,
    /// The freshly allocated right sibling.
    pub right: PageId,
    /// `btpo_level` of both pages (0 = leaf).
    pub level: u8,
    /// Slots `[copy_start_slot, slot_count)` of the left page move right.
    pub copy_start_slot: u16,
    /// LSN of the emitted `BTreeSplitPrepare` record; also the left page's
    /// `pd_lsn` after Prepare — the Copy step's idempotency anchor.
    pub prepare_lsn: Lsn,
}

/// A handle on one B+Tree index rooted at a meta page.
///
/// Cheap to construct: [`BTreeIndex::open`] reads the current
/// `(root_page_id, tree_level)` from the meta page, so after a restart (or
/// for the transient handles the `AccessMethod` glue builds per call) the
/// handle picks up the on-disk root.
pub struct BTreeIndex {
    buffer_pool: Arc<BufferPool>,
    wal_writer: Arc<WalWriter>,
    rel_oid: Oid,
    meta_page: PageId,
    root_page: PageId,
    tree_level: u8,
    key_type: ColumnType,
}

impl BTreeIndex {
    /// Assemble a handle from already-materialized on-disk state (bulk
    /// load). Does no I/O: the caller owns the meta/root pages and writes
    /// the meta record separately.
    pub(crate) fn from_parts(
        buffer_pool: Arc<BufferPool>,
        wal_writer: Arc<WalWriter>,
        rel_oid: Oid,
        meta_page: PageId,
        root_page: PageId,
        tree_level: u8,
        key_type: ColumnType,
    ) -> Self {
        Self {
            buffer_pool,
            wal_writer,
            rel_oid,
            meta_page,
            root_page,
            tree_level,
            key_type,
        }
    }

    /// Create a brand-new index: allocate the meta page and a root leaf,
    /// make both durable with post-image `FullPageImage` records (the same
    /// pattern the heap uses for page initialization — a freelist-reused
    /// page must recover as freshly initialized, not as its previous
    /// tenant's bytes), then log the first meta record `(root, 0)` as a
    /// `BTreeInsert`.
    pub fn create(
        buffer_pool: Arc<BufferPool>,
        wal_writer: Arc<WalWriter>,
        rel_oid: Oid,
        key_type: ColumnType,
    ) -> Result<Self> {
        if !is_supported_key_type(key_type) {
            return Err(BTreeError::InvalidArgument(format!(
                "unsupported index key type: {key_type:?}"
            )));
        }

        let meta_page = {
            let mut guard = buffer_pool.new_page()?;
            let page_id = guard.page_id();
            let page = as_page_mut(&mut guard);
            // The meta page is not a tree page: level 0, no LEAF/ROOT flags.
            BtreePage::init(page, 0, 0);
            log_page_init(&wal_writer, page_id, page)?;
            page_id
        };

        let root_page = {
            let mut guard = buffer_pool.new_page()?;
            let page_id = guard.page_id();
            let page = as_page_mut(&mut guard);
            BtreePage::init(page, 0, BTREE_FLAG_LEAF | BTREE_FLAG_ROOT);
            log_page_init(&wal_writer, page_id, page)?;
            page_id
        };

        let mut index = Self {
            buffer_pool,
            wal_writer,
            rel_oid,
            meta_page,
            root_page,
            tree_level: 0,
            key_type,
        };
        index.write_meta_record()?;
        Ok(index)
    }

    /// Open an existing index from its meta page, recovering the current
    /// root and tree level from the last meta record.
    pub fn open(
        buffer_pool: Arc<BufferPool>,
        wal_writer: Arc<WalWriter>,
        rel_oid: Oid,
        meta_page: PageId,
        key_type: ColumnType,
    ) -> Result<Self> {
        let (root_page, tree_level) = {
            let guard = buffer_pool.pin(meta_page)?;
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
            let slot_count = SlottedPage::slot_count(page);
            if slot_count == 0 {
                return Err(BTreeError::Corrupted(format!(
                    "meta page {meta_page} holds no root record"
                )));
            }
            let bytes = SlottedPage::tuple(page, (slot_count - 1) as u16)?.ok_or_else(|| {
                BTreeError::Corrupted(format!("meta page {meta_page} slot unreadable"))
            })?;
            page::decode_meta_record(bytes)?
        };
        if tree_level > 0x0F {
            return Err(BTreeError::Corrupted(format!(
                "meta page {meta_page} records tree level {tree_level}"
            )));
        }
        Ok(Self {
            buffer_pool,
            wal_writer,
            rel_oid,
            meta_page,
            root_page,
            tree_level: tree_level as u8,
            key_type,
        })
    }

    /// Re-read the meta page and refresh the cached `root_page` /
    /// `tree_level`, returning the current root. Used by `split_commit`'s
    /// generational check (another handle may have promoted the root since
    /// this handle cached it).
    fn refresh_root_from_meta(&mut self) -> Result<PageId> {
        let guard = self.buffer_pool.pin(self.meta_page)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        let slot_count = SlottedPage::slot_count(page);
        if slot_count == 0 {
            return Err(BTreeError::Corrupted(format!(
                "meta page {} holds no root record",
                self.meta_page
            )));
        }
        let bytes = SlottedPage::tuple(page, (slot_count - 1) as u16)?.ok_or_else(|| {
            BTreeError::Corrupted(format!("meta page {} slot unreadable", self.meta_page))
        })?;
        let (root_page, tree_level) = page::decode_meta_record(bytes)?;
        if tree_level > 0x0F {
            return Err(BTreeError::Corrupted(format!(
                "meta page {} records tree level {tree_level}",
                self.meta_page
            )));
        }
        self.root_page = root_page;
        self.tree_level = tree_level as u8;
        Ok(root_page)
    }

    /// The meta page of this index (also the relation's `first_page`).
    pub fn meta_page(&self) -> PageId {
        self.meta_page
    }

    /// The current root page, as of handle construction or the last root
    /// split performed through this handle.
    pub fn root_page(&self) -> PageId {
        self.root_page
    }

    /// The current tree level (0 = root is a leaf).
    pub fn tree_level(&self) -> u8 {
        self.tree_level
    }

    /// The indexed column type.
    pub fn key_type(&self) -> ColumnType {
        self.key_type
    }

    /// The relation OID this index belongs to.
    pub fn rel_oid(&self) -> Oid {
        self.rel_oid
    }

    /// Free space on a page (test support: crash tests fill a page until its
    /// next insert would split, then drive the split steps manually).
    pub fn page_free_space(&self, page_id: PageId) -> Result<usize> {
        let guard = self.buffer_pool.pin(page_id)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        BtreePage::level(page)?; // geometry check
        Ok(SlottedPage::free_space(page))
    }

    /// Append a meta record `(root_page, tree_level)` as the new
    /// authoritative root pointer, WAL-logged as `BTreeInsert` (meta tuple =
    /// 10-byte record, per the `BTreeInsertRecord` payload contract).
    pub(crate) fn write_meta_record(&mut self) -> Result<()> {
        let mut guard = self.buffer_pool.pin_mut(self.meta_page)?;
        let page = as_page_mut(&mut guard);
        let slot = SlottedPage::slot_count(page) as u16;
        let record_bytes = page::encode_meta_record(self.root_page, self.tree_level as u16);
        // level/flags 0/0: a fresh meta page initializes with no tree flags.
        let rec = WalRecord::btree_insert(self.meta_page, slot, 0, 0, record_bytes.clone())?;
        let lsn = self.wal_writer.append(rec)?;
        BtreePage::insert_entry_at(page, slot, &record_bytes)?;
        stamp_pd_lsn(page, lsn);
        Ok(())
    }

    // ------------------------------------------------------------------
    // Read path
    // ------------------------------------------------------------------

    /// Point lookup: return the heap TID of the first entry with `key`.
    pub fn lookup(&self, key: &[u8]) -> Result<Option<Tid>> {
        let probe_tid = Tid {
            page_id: PageId::INVALID,
            slot_id: 0,
        };
        let (mut leaf, _, _) = self.descend_to_leaf(key, &probe_tid)?;
        let mut slot = {
            let guard = self.buffer_pool.pin(leaf)?;
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
            leaf_lower_bound(page, key, &probe_tid)? as u16
        };
        // The entry that lower_bound points at is the global first entry
        // `>= (key, -infinity)`; when the page is exhausted it is the first
        // entry of the next non-empty sibling.
        let mut hops = 0usize;
        loop {
            let guard = self.buffer_pool.pin(leaf)?;
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
            let count = SlottedPage::slot_count(page) as u16;
            if slot < count {
                let (k, tid) = page::decode_leaf_entry(entry_bytes(page, slot)?)?;
                return Ok(if k == key { Some(tid) } else { None });
            }
            let next = BtreePage::next(page)?;
            drop(guard);
            if next == PageId::INVALID {
                return Ok(None);
            }
            leaf = next;
            slot = 0;
            hops += 1;
            if hops > MAX_CHAIN_HOPS {
                return Err(BTreeError::Corrupted(
                    "leaf sibling chain exceeds hop bound (cycle?)".to_string(),
                ));
            }
        }
    }

    /// Range scan over the leaf chain: every entry with
    /// `start <= key < end` (an open side is unbounded), in key order.
    ///
    /// Walks `btpo_next`, so entries on right siblings whose downlink was
    /// lost to a crash are still reached (§13.2 Blink semantics).
    pub fn range_scan(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<Vec<(Vec<u8>, Tid)>> {
        let mut out = Vec::new();
        let probe_tid = Tid {
            page_id: PageId::INVALID,
            slot_id: 0,
        };
        let (mut leaf, _, _) = self.descend_to_leaf(start.unwrap_or(&[]), &probe_tid)?;
        let mut slot = match start {
            Some(s) => {
                let guard = self.buffer_pool.pin(leaf)?;
                let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
                leaf_lower_bound(page, s, &probe_tid)? as u16
            }
            None => 0,
        };
        let mut hops = 0usize;
        loop {
            let guard = self.buffer_pool.pin(leaf)?;
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
            let count = SlottedPage::slot_count(page) as u16;
            while slot < count {
                let (k, tid) = page::decode_leaf_entry(entry_bytes(page, slot)?)?;
                if let Some(e) = end {
                    if k >= e {
                        return Ok(out);
                    }
                }
                out.push((k.to_vec(), tid));
                slot += 1;
            }
            let next = BtreePage::next(page)?;
            drop(guard);
            if next == PageId::INVALID {
                return Ok(out);
            }
            leaf = next;
            slot = 0;
            hops += 1;
            if hops > MAX_CHAIN_HOPS {
                return Err(BTreeError::Corrupted(
                    "leaf sibling chain exceeds hop bound (cycle?)".to_string(),
                ));
            }
        }
    }

    /// Descend from the root to the leaf that owns the probe `(key, tid)`,
    /// returning `(leaf, path, hopped)`: `path` holds the internal pages from
    /// root to the leaf's parent (split Commit pops it), and `hopped` records
    /// whether any right hop was taken. A right hop means the descent
    /// undershot because a downlink is missing (a split whose Commit was
    /// lost); splitting such a page is unsupported in M2b — finishing
    /// incomplete splits is M2c work. In a complete tree the internal
    /// descent never right-hops, so inserts never hit that guard.
    ///
    /// Lookups/range scans probe with `tid = Tid::INVALID` (the minimum);
    /// inserts/deletes probe with the entry's real TID.
    ///
    /// Latch coupling skeleton (§13.2): at every level the child page's
    /// guard is acquired (on the next loop iteration) before the parent's is
    /// released.
    pub fn descend_to_leaf(&self, key: &[u8], tid: &Tid) -> Result<(PageId, Vec<PageId>, bool)> {
        let mut path = Vec::new();
        let mut cur = self.root_page;
        let mut hopped = false;
        loop {
            let (level, child) = {
                let guard = self.buffer_pool.pin(cur)?;
                let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
                let level = BtreePage::level(page)?;
                let child = if level == 0 {
                    None
                } else {
                    Some(find_child(page, key)?)
                };
                (level, child)
            };
            // Position `cur` on the chain: the internal descent navigates by
            // key only (separator keys can be stale — a leftmost child's low
            // key decreases without updating the parent — and duplicate keys
            // can span siblings), so the exact page is found by walking the
            // sibling chain both ways. Right hops are the Blink mechanism
            // (§13.2); left hops cover stale separators.
            cur = self.walk_to_position(cur, key, tid, level, &mut hopped)?;
            match child {
                None => return Ok((cur, path, hopped)),
                Some(child) => {
                    path.push(cur);
                    cur = child;
                }
            }
        }
    }

    /// Walk the sibling chain at one tree level until `cur` owns the probe:
    /// `first_entry(cur) <= probe < first_entry(next)`.
    ///
    /// Leaf level compares the full `(key, tid)` order (duplicates are
    /// disambiguated by TID); internal levels compare keys strictly — equal
    /// keys neither dominate nor yield, so an internal page whose subtree
    /// may contain the probe by key is never hopped over (the leaf-level
    /// walk resolves the exact position; the sibling chains are contiguous
    /// across parents).
    ///
    /// Right hops set `hopped`: they only happen when the parent descent
    /// undershot, i.e. a downlink is missing (recovered incomplete split).
    /// An empty sibling (Prepare without Copy) owns no keys and is never
    /// hopped onto.
    fn walk_to_position(
        &self,
        mut cur: PageId,
        key: &[u8],
        tid: &Tid,
        level: u8,
        hopped: &mut bool,
    ) -> Result<PageId> {
        let mut hops = 0usize;
        loop {
            let mut moved = false;

            // Left: if cur's first entry is greater than the probe, the
            // probe sorts before cur (stale separator or duplicate run).
            // Leaves compare the full `(key, tid)` order; internal pages
            // compare keys strictly.
            let prev = {
                let guard = self.buffer_pool.pin(cur)?;
                let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
                let dominated = if SlottedPage::slot_count(page) == 0 {
                    false
                } else if level == 0 {
                    let (fk, ft) = page::decode_leaf_entry(entry_bytes(page, 0)?)?;
                    (fk, ft) > (key, *tid)
                } else {
                    let (fk, _) = page::decode_internal_entry(entry_bytes(page, 0)?)?;
                    fk > key
                };
                if dominated {
                    Some(BtreePage::prev(page)?)
                } else {
                    None
                }
            };
            if let Some(prev) = prev {
                if prev != PageId::INVALID {
                    cur = prev;
                    moved = true;
                    hops += 1;
                }
            }

            // Right: if the next sibling's first entry sorts at or below the
            // probe, the probe belongs to it or further right.
            let next = {
                let guard = self.buffer_pool.pin(cur)?;
                let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
                BtreePage::next(page)?
            };
            if next != PageId::INVALID {
                let hop = {
                    let guard = self.buffer_pool.pin(next)?;
                    let page: &[u8; PAGE_SIZE] =
                        guard.page().try_into().expect("frame is PAGE_SIZE");
                    match first_entry_key(page)? {
                        None => false, // empty twin (Prepare without Copy)
                        Some(fk) => {
                            if level == 0 {
                                let bytes = entry_bytes(page, 0)?;
                                let (fk, ft) = page::decode_leaf_entry(bytes)?;
                                (fk, ft) <= (key, *tid)
                            } else {
                                // An empty first key is the -infinity marker
                                // of a parent's leftmost child: it owns the
                                // smallest keys of its parent's range, so a
                                // real probe never sorts to its right. Only a
                                // real first key (a not-yet-linked split
                                // twin's separator) can force a right hop.
                                !fk.is_empty() && fk.as_slice() <= key
                            }
                        }
                    }
                };
                if hop {
                    cur = next;
                    *hopped = true;
                    moved = true;
                    hops += 1;
                }
            }

            if !moved {
                return Ok(cur);
            }
            if hops > MAX_CHAIN_HOPS {
                return Err(BTreeError::Corrupted(
                    "sibling chain exceeds hop bound (cycle?)".to_string(),
                ));
            }
        }
    }

    // ------------------------------------------------------------------
    // Insert
    // ------------------------------------------------------------------

    /// Insert `(key, tid)`. Duplicate keys are allowed; a duplicate
    /// `(key, tid)` pair is [`BTreeError::DuplicateKey`].
    ///
    /// When the target leaf has no room, it is split (3-step WAL, see the
    /// module docs) and the entry is inserted into the half it belongs to.
    pub fn insert(&mut self, key: &[u8], tid: Tid) -> Result<()> {
        if key.len() > MAX_INDEX_KEY_BYTES {
            return Err(BTreeError::KeyTooLarge(key.len()));
        }
        let entry = page::encode_leaf_entry(key, tid);
        let (leaf, path, hopped) = self.descend_to_leaf(key, &tid)?;

        let mut guard = self.buffer_pool.pin_mut(leaf)?;
        {
            let page = as_page_mut(&mut guard);
            let pos = leaf_lower_bound(page, key, &tid)?;
            let count = SlottedPage::slot_count(page);
            if pos < count {
                let (k, t) = page::decode_leaf_entry(entry_bytes(page, pos as u16)?)?;
                if k == key && t == tid {
                    return Err(BTreeError::DuplicateKey);
                }
            }
            let needed = entry.len() + 4; // entry + one line pointer
            if SlottedPage::free_space(page) >= needed {
                return self.insert_into_page(&mut guard, pos as u16, entry);
            }
        }
        drop(guard);

        if hopped {
            // The target page's own downlink may be missing (its split's
            // Commit was lost); splitting it again would orphan its right
            // twin. M2b leaves finishing incomplete splits to M2c undo.
            return Err(BTreeError::Unsupported(
                "split of a page reached via a Blink right hop (incomplete \
                 ancestor split); finishing incomplete splits is M2c work"
                    .to_string(),
            ));
        }
        self.split_and_insert(leaf, path, entry)
    }

    /// WAL-log (`BTreeInsert`) and apply an entry insert at `slot` of an
    /// already-pinned page that has room. Shared by leaf inserts, new-root
    /// initialization and meta records.
    fn insert_into_page(
        &self,
        guard: &mut PageGuardMut<'_>,
        slot: u16,
        entry: Vec<u8>,
    ) -> Result<()> {
        let page_id = guard.page_id();
        let page = as_page_mut(guard);
        let level = BtreePage::level(page)?;
        let flags = BtreePage::flags(page)?;
        let rec = WalRecord::btree_insert(page_id, slot, level, flags, entry.clone())?;
        let lsn = self.wal_writer.append(rec)?;
        BtreePage::insert_entry_at(page, slot, &entry)?;
        stamp_pd_lsn(page, lsn);
        Ok(())
    }

    /// Split the full page `left` (3-step WAL), insert `entry` into the half
    /// it belongs to, then commit the split into the parent (recursively).
    fn split_and_insert(
        &mut self,
        left: PageId,
        mut path: Vec<PageId>,
        entry: Vec<u8>,
    ) -> Result<()> {
        // Generational check BEFORE emitting any split record: if this
        // handle believes `left` is the root but the meta page has moved on
        // (another handle promoted it), fail now — `split_commit`'s
        // backstop would catch the same staleness only after Prepare/Copy
        // had already left `left` SPLIT_INCOMPLETE.
        if left == self.root_page {
            let current_root = self.refresh_root_from_meta()?;
            if current_root != left {
                return Err(BTreeError::Unsupported(format!(
                    "root page {left} is stale (meta now points at {current_root}); \
                     reopen the index handle and retry the insert"
                )));
            }
        }
        let st = self.split_prepare(left)?;
        self.split_copy(&st)?;

        // Insert the pending entry into whichever half it sorts into.
        let target = {
            let right_first = self.first_entry_bytes(st.right)?;
            if entry_cmp(&entry, &right_first, st.level == 0)? == Ordering::Less {
                st.left
            } else {
                st.right
            }
        };
        let mut guard = self.buffer_pool.pin_mut(target)?;
        let pos = {
            let page = as_page_mut(&mut guard);
            entry_lower_bound(page, &entry, st.level == 0)? as u16
        };
        self.insert_into_page(&mut guard, pos, entry)?;
        drop(guard);

        self.split_commit(&st, &mut path)
    }

    // ------------------------------------------------------------------
    // Split: the three WAL steps (§13.3), individually drivable for crash
    // tests. The online path runs them back to back via `split_and_insert`.
    // ------------------------------------------------------------------

    /// §13.3 step 1: allocate the right sibling and emit + apply
    /// `BTreeSplitPrepare` (link `left.next = right`, set
    /// `SPLIT_INCOMPLETE` on the left page, initialize the right page
    /// header). The split point is the median slot.
    ///
    /// Refuses to split a page that is itself `SPLIT_INCOMPLETE`
    /// ([`BTreeError::Unsupported`]): such a page's previous split lost its
    /// Commit, so its right twin T has no parent downlink. Splitting it
    /// again would re-point `left.next` to a *new* twin T2 and give T2 the
    /// downlink, permanently orphaning T — and M2c's incomplete-split
    /// finish (`BTreeSplitCLR`) relies on `left.next` to find T. Until M2c
    /// finishes incomplete splits, a second split of such a page is
    /// forbidden (same severity as the `hopped` guard in `insert`).
    pub fn split_prepare(&self, left: PageId) -> Result<SplitState> {
        let mut left_guard = self.buffer_pool.pin_mut(left)?;
        let (level, old_next, high_key, copy_start_slot) = {
            let page = as_page_mut(&mut left_guard);
            let level = BtreePage::level(page)?;
            if BtreePage::flags(page)? & BTREE_FLAG_SPLIT_INCOMPLETE != 0 {
                return Err(BTreeError::Unsupported(format!(
                    "page {left} is SPLIT_INCOMPLETE; a second split would orphan its \
                     uncommitted right twin (finishing incomplete splits is M2c work)"
                )));
            }
            let old_next = BtreePage::next(page)?;
            let count = SlottedPage::slot_count(page);
            if count < 2 {
                return Err(BTreeError::Corrupted(format!(
                    "cannot split page {left} with {count} entries"
                )));
            }
            let high_key = entry_key(entry_bytes(page, (count - 1) as u16)?, level)?.to_vec();
            (level, old_next, high_key, (count / 2) as u16)
        };

        let mut right_guard = self.buffer_pool.new_page()?;
        let right = right_guard.page_id();

        let rec = WalRecord::btree_split_prepare(left, right, level, old_next, high_key)?;
        let lsn = self.wal_writer.append(rec)?;
        {
            let page = as_page_mut(&mut right_guard);
            BtreePage::init_right_page(page, left, old_next, level);
            stamp_pd_lsn(page, lsn);
        }
        {
            let page = as_page_mut(&mut left_guard);
            BtreePage::apply_prepare_left(page, right)?;
            stamp_pd_lsn(page, lsn);
        }
        Ok(SplitState {
            left,
            right,
            level,
            copy_start_slot,
            prepare_lsn: lsn,
        })
    }

    /// §13.3 step 2: emit + apply `BTreeSplitCopy` — move
    /// `[copy_start_slot, slot_count)` from the left page to the right page
    /// and truncate the left LP array. `left_page_pre_lsn` anchors redo
    /// idempotency.
    ///
    /// After applying, the right page is flushed before the left guard is
    /// released: redo's Copy recomputes the moved entries from the left
    /// page's pre-copy image, so the left page's post-copy image must never
    /// be durable while the right page's is not. Both pages are pinned
    /// (eviction-proof) until the right page's flush completes.
    pub fn split_copy(&self, st: &SplitState) -> Result<Lsn> {
        let mut left_guard = self.buffer_pool.pin_mut(st.left)?;
        let mut right_guard = self.buffer_pool.pin_mut(st.right)?;
        // The anchor is read AFTER `pin_mut`: if a checkpoint landed between
        // Prepare and Copy, the pool fires an FPI for the left page inside
        // `pin_mut` and pushes its `pd_lsn` to the FPI's LSN. That is fine —
        // the anchor is "whatever `pd_lsn` is now" (Prepare LSN or the FPI
        // LSN covering the same content), the record carries it, and redo
        // compares equality: replaying the FPI restores exactly this pre-copy
        // image and stamps the same FPI LSN, so the anchor holds either way.
        // (There used to be a `debug_assert_eq!(pre_lsn, st.prepare_lsn)`
        // here; it contradicted the pool's automatic FPI and has been
        // removed.)
        let pre_lsn = page_pd_lsn(as_page_mut(&mut left_guard));

        let rec = WalRecord::btree_split_copy(st.left, st.right, st.copy_start_slot, pre_lsn)?;
        let lsn = self.wal_writer.append(rec)?;
        apply_split_copy(
            as_page_mut(&mut left_guard),
            as_page_mut(&mut right_guard),
            st.copy_start_slot,
            true,
        )?;
        stamp_pd_lsn(as_page_mut(&mut left_guard), lsn);
        stamp_pd_lsn(as_page_mut(&mut right_guard), lsn);

        // Flush the right page's post-copy image first (see the doc above).
        // Dropping the guard releases the write latch so `flush` can take a
        // read latch; the left guard stays held, so the left page cannot be
        // evicted/flushed before the right page is durable.
        drop(right_guard);
        self.buffer_pool.flush(st.right)?;
        Ok(lsn)
    }

    /// §13.3 step 3: emit + apply `BTreeSplitCommit` — insert the downlink
    /// `(separator_key, right_page)` into the parent and clear
    /// `SPLIT_INCOMPLETE` (and `ROOT`, for a root split) on the left page.
    ///
    /// `path` is the descent path recorded when the split was triggered
    /// (root..parent of `left`); the parent is popped from it. A parent
    /// without room for the downlink is split first, recursively. When
    /// `left` is the root, a new root is allocated, seeded with
    /// `(-infinity -> left)`, the meta page is updated, and the downlink
    /// lands at slot 1.
    pub fn split_commit(&mut self, st: &SplitState, path: &mut Vec<PageId>) -> Result<()> {
        let separator = {
            let bytes = self.first_entry_bytes(st.right)?;
            entry_key(&bytes, st.level)?.to_vec()
        };
        let downlink = page::encode_internal_entry(&separator, st.right);

        let (parent, slot) = if st.left == self.root_page {
            // Generational check: this handle cached `root_page` at
            // open/last-root-split, but ANOTHER handle on the same index
            // may have promoted the root since. Re-read the meta page; if
            // it no longer points at `st.left`, creating a "new root" here
            // would fork the tree (two roots, meta overwritten, half the
            // tree unreachable). Refresh the handle from the meta page and
            // fail loudly instead — the caller must reopen the handle and
            // retry. (The alternative of continuing with the stale descent
            // path has no correct parent to attach to: the path was
            // recorded when `st.left` was the root, so it is empty.)
            let current_root = self.refresh_root_from_meta()?;
            if current_root != st.left {
                return Err(BTreeError::Unsupported(format!(
                    "root page {} is stale (meta now points at {current_root}); \
                     reopen the index handle and retry the insert",
                    st.left
                )));
            }
            let new_root = self.create_new_root(st)?;
            (new_root, 1u16)
        } else {
            let mut parent = path.pop().ok_or_else(|| {
                BTreeError::Corrupted(format!(
                    "split of non-root page {} with an empty descent path",
                    st.left
                ))
            })?;
            if !self.page_fits(parent, downlink.len())? {
                // Split the parent first; the pending downlink is applied by
                // THIS split's Commit afterwards (each downlink is logged by
                // exactly one Commit record).
                let pst = self.split_prepare(parent)?;
                self.split_copy(&pst)?;
                self.split_commit(&pst, path)?;
                let right_first = {
                    let bytes = self.first_entry_bytes(pst.right)?;
                    entry_key(&bytes, pst.level)?.to_vec()
                };
                if separator.as_slice() >= right_first.as_slice() {
                    parent = pst.right;
                }
            }
            let slot = self.internal_insert_slot(parent, &separator, st.right)?;
            (parent, slot)
        };

        let rec = WalRecord::btree_split_commit(st.left, st.right, parent, separator, slot)?;
        let lsn = self.wal_writer.append(rec)?;
        {
            let mut guard = self.buffer_pool.pin_mut(parent)?;
            let page = as_page_mut(&mut guard);
            BtreePage::insert_entry_at(page, slot, &downlink)?;
            stamp_pd_lsn(page, lsn);
        }
        {
            let mut guard = self.buffer_pool.pin_mut(st.left)?;
            let page = as_page_mut(&mut guard);
            BtreePage::apply_commit_left(page)?;
            stamp_pd_lsn(page, lsn);
        }
        Ok(())
    }

    /// Allocate and seed the new root for a root split: a fresh page at
    /// `st.level + 1` holding `(-infinity -> left)` at slot 0, then a meta
    /// record pointing at it. The downlink to `st.right` is added by the
    /// caller's Commit apply at slot 1.
    ///
    /// Slot 0 of an internal page carries an **empty key as the -infinity
    /// marker** (PG's `P_HIKEY` convention): the leftmost child's low key
    /// can decrease over time (descending inserts), and a real key here
    /// would go stale and scramble the parent's key order against the
    /// sibling-chain order. Non-leftmost pages always carry a real
    /// separator at slot 0 (copied verbatim by splits), so parent markers
    /// can only go stale *low* (physical deletes), which the descent's
    /// left-walk absorbs.
    ///
    /// The new root's initialization is covered by the first `BTreeInsert`
    /// record on it (its `level`/`flags` fields let redo initialize a fresh
    /// page), so no separate init record is needed.
    fn create_new_root(&mut self, st: &SplitState) -> Result<PageId> {
        // `btpo_level` is a 4-bit field (§13.1): the 16th root promotion
        // would overflow `level` into the `btpo_flags` bits. Make the
        // implicit assumption an explicit contract instead of corrupting the
        // flags nibble (a 15-level tree of 8 KB pages is unreachable in
        // practice, so failing loudly here is always right).
        if st.level >= 0x0F {
            return Err(BTreeError::Corrupted(format!(
                "tree level {} already at the 4-bit maximum; cannot promote the root",
                st.level
            )));
        }
        let new_level = st.level + 1;

        let new_root = {
            let mut guard = self.buffer_pool.new_page()?;
            let page_id = guard.page_id();
            {
                let page = as_page_mut(&mut guard);
                BtreePage::init(page, new_level, BTREE_FLAG_ROOT);
            }
            // -infinity -> old root (see the doc above).
            let e0 = page::encode_internal_entry(&[], st.left);
            self.insert_into_page(&mut guard, 0, e0)?;
            page_id
        };

        self.root_page = new_root;
        self.tree_level = new_level;
        self.write_meta_record()?;
        Ok(new_root)
    }

    /// Does `page_id` have room for an entry of `entry_len` bytes?
    fn page_fits(&self, page_id: PageId, entry_len: usize) -> Result<bool> {
        Ok(self.page_free_space(page_id)? >= entry_len + 4)
    }

    /// Insertion slot for the downlink `(key, child)` on an internal page.
    fn internal_insert_slot(&self, page_id: PageId, key: &[u8], child: PageId) -> Result<u16> {
        let guard = self.buffer_pool.pin(page_id)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        Ok(internal_lower_bound(page, key, child)? as u16)
    }

    /// Read the raw bytes of a page's first entry.
    fn first_entry_bytes(&self, page_id: PageId) -> Result<Vec<u8>> {
        let guard = self.buffer_pool.pin(page_id)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        if SlottedPage::slot_count(page) == 0 {
            return Err(BTreeError::Corrupted(format!(
                "page {page_id} has no entries"
            )));
        }
        Ok(entry_bytes(page, 0)?.to_vec())
    }

    // ------------------------------------------------------------------
    // Delete
    // ------------------------------------------------------------------

    /// Physically remove the exact `(key, tid)` entry (`BTreeDelete`; M2b
    /// has no page merge).
    pub fn delete(&mut self, key: &[u8], tid: Tid) -> Result<()> {
        let (leaf, _, _) = self.descend_to_leaf(key, &tid)?;
        let mut guard = self.buffer_pool.pin_mut(leaf)?;
        let page = as_page_mut(&mut guard);
        let pos = leaf_lower_bound(page, key, &tid)?;
        let count = SlottedPage::slot_count(page);
        if pos >= count {
            return Err(BTreeError::EntryNotFound);
        }
        let (k, t) = page::decode_leaf_entry(entry_bytes(page, pos as u16)?)?;
        if k != key || t != tid {
            return Err(BTreeError::EntryNotFound);
        }
        let slot = pos as u16;
        let rec = WalRecord::btree_delete(leaf, slot)?;
        let lsn = self.wal_writer.append(rec)?;
        BtreePage::remove_entry_at(page, slot)?;
        stamp_pd_lsn(page, lsn);
        Ok(())
    }

    // ------------------------------------------------------------------
    // Structural validation (tests / diagnostics)
    // ------------------------------------------------------------------

    /// Strict structural validation, intended for tests and diagnostics.
    ///
    /// Checks, recursively from the root: page geometry, `btpo_level`
    /// consistency, entries strictly sorted in full `(key, trailer)` order,
    /// and **adjacent subtree ranges strictly increasing** — each child's
    /// last leaf entry must sort below the next child's first leaf entry.
    /// The boundary check compares full entries rather than parent
    /// separator keys: separator keys can legitimately go stale (physical
    /// deletes raise a page's first key; duplicate keys at a split point
    /// legitimately live on both sides), but the sibling-chain order is the
    /// ground truth the descent walk relies on. Finally, the leaf chain
    /// walked from the leftmost leaf must match the root-reachable leaves,
    /// in order, and no page may be `SPLIT_INCOMPLETE`.
    ///
    /// An index carrying a recovered incomplete split fails this check on
    /// purpose (its right twin is unreachable from the root); crash tests
    /// assert the weaker chain/lookup properties instead.
    pub fn validate(&self) -> Result<()> {
        // An empty index (root leaf with no entries, e.g. right after
        // `create` or a bulk load of zero rows) is trivially valid.
        let root_slot_count = {
            let guard = self.buffer_pool.pin(self.root_page)?;
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
            BtreePage::level(page)?;
            SlottedPage::slot_count(page)
        };
        if root_slot_count == 0 {
            if self.tree_level != 0 {
                return Err(BTreeError::Corrupted(format!(
                    "empty root page {} at tree level {}",
                    self.root_page, self.tree_level
                )));
            }
            return Ok(());
        }

        let mut leaves = Vec::new();
        self.validate_page(self.root_page, self.tree_level, &mut leaves)?;

        // The leaf chain from the leftmost leaf must visit exactly the
        // root-reachable leaves, in order.
        let (leftmost, _, _) = self.descend_to_leaf(
            &[],
            &Tid {
                page_id: PageId::INVALID,
                slot_id: 0,
            },
        )?;
        let mut chain = Vec::new();
        let mut cur = leftmost;
        let mut hops = 0usize;
        loop {
            chain.push(cur);
            let guard = self.buffer_pool.pin(cur)?;
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
            let next = BtreePage::next(page)?;
            drop(guard);
            if next == PageId::INVALID {
                break;
            }
            cur = next;
            hops += 1;
            if hops > MAX_CHAIN_HOPS {
                return Err(BTreeError::Corrupted(
                    "leaf sibling chain exceeds hop bound (cycle?)".to_string(),
                ));
            }
        }
        if chain != leaves {
            return Err(BTreeError::Corrupted(format!(
                "leaf chain {chain:?} disagrees with root-reachable leaves {leaves:?}"
            )));
        }
        Ok(())
    }

    /// Recursive helper for [`BTreeIndex::validate`]: check one subtree,
    /// append its leaves (in order) to `leaves`, and return the subtree's
    /// first and last **leaf** entry bytes (full `(key, tid)` order
    /// boundaries).
    fn validate_page(
        &self,
        page_id: PageId,
        expect_level: u8,
        leaves: &mut Vec<PageId>,
    ) -> Result<(Vec<u8>, Vec<u8>)> {
        let guard = self.buffer_pool.pin(page_id)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        let level = BtreePage::level(page)?;
        let flags = BtreePage::flags(page)?;
        if level != expect_level {
            return Err(BTreeError::Corrupted(format!(
                "page {page_id} at level {level}, expected {expect_level}"
            )));
        }
        if flags & BTREE_FLAG_SPLIT_INCOMPLETE != 0 {
            return Err(BTreeError::Corrupted(format!(
                "page {page_id} still SPLIT_INCOMPLETE"
            )));
        }
        if (level == 0) != (flags & BTREE_FLAG_LEAF != 0) {
            return Err(BTreeError::Corrupted(format!(
                "page {page_id} level {level} disagrees with LEAF flag"
            )));
        }
        let count = SlottedPage::slot_count(page);
        if count == 0 {
            return Err(BTreeError::Corrupted(format!("page {page_id} is empty")));
        }

        // Entries must be strictly sorted in full `(key, trailer)` order
        // (duplicate keys are allowed; the trailer disambiguates).
        for slot in 1..count as u16 {
            let prev = entry_bytes(page, slot - 1)?;
            let cur = entry_bytes(page, slot)?;
            if entry_cmp(cur, prev, level == 0)? != Ordering::Greater {
                return Err(BTreeError::Corrupted(format!(
                    "page {page_id} entries out of order at slot {slot}"
                )));
            }
        }

        if level == 0 {
            leaves.push(page_id);
            return Ok((
                entry_bytes(page, 0)?.to_vec(),
                entry_bytes(page, (count - 1) as u16)?.to_vec(),
            ));
        }

        // Internal page: recurse into every child and require adjacent
        // subtree ranges to strictly increase.
        let mut subtree_first: Option<Vec<u8>> = None;
        let mut prev_last: Option<Vec<u8>> = None;
        for slot in 0..count as u16 {
            let (_, child) = page::decode_internal_entry(entry_bytes(page, slot)?)?;
            let (child_first, child_last) = self.validate_page(child, level - 1, leaves)?;
            if let Some(prev) = &prev_last {
                if entry_cmp(&child_first, prev, true)? != Ordering::Greater {
                    return Err(BTreeError::Corrupted(format!(
                        "page {page_id} child {child} range overlaps or is out of order"
                    )));
                }
            }
            if subtree_first.is_none() {
                subtree_first = Some(child_first);
            }
            prev_last = Some(child_last);
        }
        Ok((
            subtree_first.expect("internal page has children"),
            prev_last.expect("internal page has children"),
        ))
    }
}

// ----------------------------------------------------------------------
// Free helpers
// ----------------------------------------------------------------------

/// Apply the Copy transformation (§13.3 step 2): move every entry of the
/// left page at `>= copy_start_slot` onto the right page (appended in slot
/// order), then **rebuild** the left page with the entries it keeps.
///
/// A bare LP-array truncation would leave the moved tuple bytes as dead
/// space (`pd_upper` never recovers), so the left page would still be
/// effectively full and the very insert that triggered the split would not
/// fit. Rebuilding compacts the kept entries back to a fresh page while
/// preserving `pd_lsn`, `btpo_prev`/`btpo_next`, level and flags. The
/// transformation is deterministic, so the online path and the
/// `BTreeSplitCopy` redo handler produce byte-identical pages.
///
/// `move_to_right` is `false` only for the redo interleaving where the
/// right page's post-copy image is already durable (it then holds the
/// entries, and only the left page's rebuild is missing).
pub(crate) fn apply_split_copy(
    left_page: &mut [u8; PAGE_SIZE],
    right_page: &mut [u8; PAGE_SIZE],
    copy_start_slot: u16,
    move_to_right: bool,
) -> Result<()> {
    let count = SlottedPage::slot_count(left_page) as u16;
    // `copy_start_slot == count` is the no-op case: the copy was already
    // applied (the left page holds exactly the kept entries), so the rebuild
    // below deterministically re-packs the same content. `0` or beyond the
    // slot count is genuine corruption.
    if copy_start_slot == 0 || copy_start_slot > count {
        return Err(BTreeError::Corrupted(format!(
            "copy_start_slot {copy_start_slot} outside slot count {count}"
        )));
    }
    // Collect first, so the borrow of the left page ends before the right
    // page is mutated and the rebuild starts from a clean slate.
    let mut kept: Vec<Vec<u8>> = Vec::new();
    let mut moved: Vec<Vec<u8>> = Vec::new();
    for slot in 0..count {
        let bytes = entry_bytes(left_page, slot)?.to_vec();
        if slot < copy_start_slot {
            kept.push(bytes);
        } else {
            moved.push(bytes);
        }
    }
    if move_to_right {
        for entry in &moved {
            // The right page starts empty (Prepare initialized it), so heap
            // append is slot-deterministic.
            SlottedPage::add_tuple(right_page, entry)?;
        }
    }

    // Rebuild the left page, preserving its identity fields.
    let pd_lsn = page_pd_lsn(left_page);
    let prev = BtreePage::prev(left_page)?;
    let next = BtreePage::next(left_page)?;
    let level = BtreePage::level(left_page)?;
    let flags = BtreePage::flags(left_page)?;
    BtreePage::init(left_page, level, flags);
    BtreePage::set_prev(left_page, prev);
    BtreePage::set_next(left_page, next);
    for entry in &kept {
        SlottedPage::add_tuple(left_page, entry)?;
    }
    set_page_pd_lsn(left_page, pd_lsn);
    Ok(())
}

/// Binary search: first slot whose entry is `>= (key, tid)` in full
/// `(key, tid)` order — the insertion point, or the exact match candidate.
fn leaf_lower_bound(page: &[u8; PAGE_SIZE], key: &[u8], tid: &Tid) -> Result<usize> {
    let count = SlottedPage::slot_count(page);
    let (mut lo, mut hi) = (0usize, count);
    while lo < hi {
        let mid = (lo + hi) / 2;
        let (k, t) = page::decode_leaf_entry(entry_bytes(page, mid as u16)?)?;
        if k.cmp(key).then(t.cmp(tid)) == Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// Binary search on an internal page: first slot whose entry is
/// `>= (key, child)` in full `(key, child_page_id)` order.
fn internal_lower_bound(page: &[u8; PAGE_SIZE], key: &[u8], child: PageId) -> Result<usize> {
    let count = SlottedPage::slot_count(page);
    let (mut lo, mut hi) = (0usize, count);
    while lo < hi {
        let mid = (lo + hi) / 2;
        let (k, c) = page::decode_internal_entry(entry_bytes(page, mid as u16)?)?;
        if k.cmp(key).then(c.cmp(&child)) == Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// Binary search for the insertion point of an encoded `entry` on a page
/// whose entries use the same encoding (`leaf` selects the trailer size).
fn entry_lower_bound(page: &[u8; PAGE_SIZE], entry: &[u8], leaf: bool) -> Result<usize> {
    let count = SlottedPage::slot_count(page);
    let (mut lo, mut hi) = (0usize, count);
    while lo < hi {
        let mid = (lo + hi) / 2;
        if entry_cmp(entry_bytes(page, mid as u16)?, entry, leaf)? == Ordering::Less {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

/// Compare two encoded entries in full `(key, trailer)` order.
fn entry_cmp(a: &[u8], b: &[u8], leaf: bool) -> Result<Ordering> {
    if leaf {
        let (ka, ta) = page::decode_leaf_entry(a)?;
        let (kb, tb) = page::decode_leaf_entry(b)?;
        Ok(ka.cmp(kb).then(ta.cmp(&tb)))
    } else {
        let (ka, ca) = page::decode_internal_entry(a)?;
        let (kb, cb) = page::decode_internal_entry(b)?;
        Ok(ka.cmp(kb).then(ca.cmp(&cb)))
    }
}

/// Extract the key of an encoded entry (trailer size selected by `level`).
fn entry_key(bytes: &[u8], level: u8) -> Result<&[u8]> {
    if level == 0 {
        Ok(page::decode_leaf_entry(bytes)?.0)
    } else {
        Ok(page::decode_internal_entry(bytes)?.0)
    }
}

/// The first entry's key on a page, or `None` for an empty page.
fn first_entry_key(page: &[u8; PAGE_SIZE]) -> Result<Option<Vec<u8>>> {
    if SlottedPage::slot_count(page) == 0 {
        return Ok(None);
    }
    let level = BtreePage::level(page)?;
    Ok(Some(entry_key(entry_bytes(page, 0)?, level)?.to_vec()))
}

/// Descent rule on an internal page: the last entry with `key <= probe`,
/// or entry 0 when the probe is smaller than every key (the leftmost child
/// covers `-infinity`).
fn find_child(page: &[u8; PAGE_SIZE], key: &[u8]) -> Result<PageId> {
    let count = SlottedPage::slot_count(page);
    if count == 0 {
        return Err(BTreeError::Corrupted(
            "internal page with no entries".to_string(),
        ));
    }
    // First slot with entry.key > probe; the child is the slot before it.
    let (mut lo, mut hi) = (0usize, count);
    while lo < hi {
        let mid = (lo + hi) / 2;
        let (k, _) = page::decode_internal_entry(entry_bytes(page, mid as u16)?)?;
        if k <= key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    let slot = lo.saturating_sub(1) as u16;
    let (_, child) = page::decode_internal_entry(entry_bytes(page, slot)?)?;
    Ok(child)
}

/// Read the raw bytes of the entry at `slot` (geometry-checked).
fn entry_bytes(page: &[u8; PAGE_SIZE], slot: u16) -> Result<&[u8]> {
    SlottedPage::tuple(page, slot)?
        .ok_or_else(|| BTreeError::Corrupted(format!("slot {slot} does not hold a live entry")))
}

/// Reinterpret a write guard's page bytes as a fixed-size page array.
fn as_page_mut<'g>(guard: &'g mut PageGuardMut<'_>) -> &'g mut [u8; PAGE_SIZE] {
    guard
        .page_mut()
        .try_into()
        .expect("buffer frame is exactly PAGE_SIZE")
}

/// Append a post-image `FullPageImage` of a freshly initialized page and
/// stamp its `pd_lsn` — the durability anchor for page initialization (same
/// pattern as the heap's `log_page_init`).
pub(crate) fn log_page_init(
    wal_writer: &Arc<WalWriter>,
    page_id: PageId,
    page: &mut [u8; PAGE_SIZE],
) -> Result<()> {
    let image = page.to_vec();
    let lsn = wal_writer.append(WalRecord::full_page_image(page_id, image)?)?;
    stamp_pd_lsn(page, lsn);
    Ok(())
}

/// Advance the page's authoritative `pd_lsn` to `max(lsn, current)`.
fn stamp_pd_lsn(page: &mut [u8; PAGE_SIZE], lsn: Lsn) {
    let new_lsn = lsn.max(page_pd_lsn(page));
    set_page_pd_lsn(page, new_lsn);
}
