//! Bottom-up bulk load for blocking `CREATE INDEX` (coding-plan Stage M
//! row "CREATE INDEX 阻塞式").
//!
//! Given the full `(key_bytes, tid)` entry set of a heap scan, the loader:
//!
//! 1. sorts entries in full `(key, tid)` order,
//! 2. packs **leaf pages left to right** (each `LEAF`, linked by
//!    `btpo_prev`/`btpo_next`),
//! 3. packs each internal level bottom-up: a page's slot-0 downlink carries
//!    the empty **-infinity key** when the page is the first of its level
//!    (the leftmost-spine convention of [`crate::index`]); every other
//!    downlink carries the child's subtree low key,
//! 4. finishes by appending the single meta record `(root, tree_level)`.
//!
//! # Fill policy
//!
//! Pages are packed **full** (the next entry goes to a fresh page when it no
//! longer fits — effectively a 100% fill factor). This is the simplest
//! deterministic rule and matches the loader's purpose: a one-shot,
//! read-optimized image. Later inserts split pages through the normal
//! (battle-tested) split path, which rebalances occupancy toward ~50% where
//! the write pattern needs it.
//!
//! # WAL and crash recovery
//!
//! Every built page (meta, leaves, internal pages, root) is made durable
//! with exactly one **post-image `FullPageImage` record**, emitted after the
//! page's final content (entries + sibling links + level/flags) is set, and
//! the page's `pd_lsn` is stamped with that record's LSN — the same
//! durability pattern the heap uses for page initialization. The
//! alternatives lose on every axis that matters here: one `BTreeInsert` per
//! entry would be a million tiny records (the slow path the loader exists to
//! avoid), and the 3-step split protocol does not apply (its Copy step
//! recomputes *moved* content from an existing left page — bulk pages hold
//! *new* content). One 8 KB record per page is ~19 MB of WAL for a
//! 1M-entry index and replays through the stock, idempotent
//! `FullPageImageRedoHandler`.
//!
//! The **meta record is written last**. A crash anywhere earlier leaves only
//! FPI-covered pages whose tree pointer was never published: with no meta
//! record (and, at the engine layer, no `pg_index`/`pg_class` rows — those
//! are committed after the build) the half-built index is unreachable leaked
//! pages, never a corrupt half-state. A crash after the meta record means
//! every page of the tree was already FPI'd, so recovery replays to the
//! complete index.

use std::sync::Arc;

use pg_am_heap::slotted_page::SlottedPage;
use pg_am_heap::tuple::ColumnType;
use pg_storage::buffer_pool::BufferPool;
use pg_storage::page::PAGE_HEADER_SIZE;
use pg_storage::types::{Oid, PageId, Tid, PAGE_SIZE};
use pg_storage::wal::WalWriter;

use crate::error::{BTreeError, Result};
use crate::index::{log_page_init, BTreeIndex};
use crate::key::{is_supported_key_type, MAX_INDEX_KEY_BYTES};
use crate::page::{self, BtreePage, BTREE_FLAG_LEAF, BTREE_FLAG_ROOT, BTREE_SPECIAL_SIZE};

/// Usable bytes for entries on one page (page minus header and special
/// space).
const USABLE_SPACE: usize = PAGE_SIZE - PAGE_HEADER_SIZE - BTREE_SPECIAL_SIZE;

/// One built page: its page id and its subtree's low key (the first leaf
/// key reachable from it), which becomes its separator in the parent level.
struct BuiltPage {
    page_id: PageId,
    low_key: Vec<u8>,
}

/// Bulk-load `entries` into a brand-new index for `rel_oid`, returning the
/// open handle. See the module docs for the packing, WAL and crash
/// semantics.
pub fn build(
    buffer_pool: &Arc<BufferPool>,
    wal_writer: &Arc<WalWriter>,
    rel_oid: Oid,
    key_type: ColumnType,
    mut entries: Vec<(Vec<u8>, Tid)>,
) -> Result<BTreeIndex> {
    if !is_supported_key_type(key_type) {
        return Err(BTreeError::InvalidArgument(format!(
            "unsupported index key type: {key_type:?}"
        )));
    }
    for (key, _) in &entries {
        if key.len() > MAX_INDEX_KEY_BYTES {
            return Err(BTreeError::KeyTooLarge(key.len()));
        }
    }
    entries.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    // A heap scan can never produce the same TID twice; dedup is cheap
    // insurance for other callers, since a duplicated `(key, tid)` entry
    // would violate the strict page-ordering invariant.
    entries.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);

    // The meta page exists from the start (FPI'd, empty) but its root
    // record is written only after the whole tree is durable (module docs).
    let meta_page = alloc_and_log_page(buffer_pool, wal_writer, 0, 0, &[])?;

    // Leaf level. An empty entry set builds a single empty root leaf —
    // identical in shape to `BTreeIndex::create`.
    let leaf_entries: Vec<Vec<u8>> = entries
        .iter()
        .map(|(k, t)| page::encode_leaf_entry(k, *t))
        .collect();
    let mut tree_level: u8 = 0;
    let mut current = pack_level(
        buffer_pool,
        |page_id, prev, next, run: &[Vec<u8>]| {
            let mut guard = buffer_pool.pin_mut(page_id)?;
            let page: &mut [u8; PAGE_SIZE] =
                guard.page_mut().try_into().expect("frame is PAGE_SIZE");
            BtreePage::init(page, 0, BTREE_FLAG_LEAF);
            BtreePage::set_prev(page, prev);
            BtreePage::set_next(page, next);
            for item in run {
                SlottedPage::add_tuple(page, item)?;
            }
            log_page_init(wal_writer, page_id, page)?;
            let low_key = if run.is_empty() {
                Vec::new()
            } else {
                page::decode_leaf_entry(&run[0])?.0.to_vec()
            };
            Ok(low_key)
        },
        &leaf_entries,
        |item| item.len(),
    )?;

    // Internal levels, bottom-up, until a single root page remains.
    while current.len() > 1 {
        tree_level += 1;
        let children = current;
        current = pack_level(
            buffer_pool,
            |page_id, prev, next, run: &[BuiltPage]| {
                let mut guard = buffer_pool.pin_mut(page_id)?;
                let page: &mut [u8; PAGE_SIZE] =
                    guard.page_mut().try_into().expect("frame is PAGE_SIZE");
                BtreePage::init(page, tree_level, 0);
                BtreePage::set_prev(page, prev);
                BtreePage::set_next(page, next);
                for (i, child) in run.iter().enumerate() {
                    // Slot 0 of the FIRST page of the level is the -infinity
                    // marker (leftmost-spine convention); every other
                    // downlink carries the child's real subtree low key.
                    let key: &[u8] = if i == 0 && prev == PageId::INVALID {
                        &[]
                    } else {
                        &child.low_key
                    };
                    let entry = page::encode_internal_entry(key, child.page_id);
                    SlottedPage::add_tuple(page, &entry)?;
                }
                log_page_init(wal_writer, page_id, page)?;
                Ok(run[0].low_key.clone())
            },
            &children,
            |child| child.low_key.len() + 8,
        )?;
    }

    let root = current.into_iter().next().expect("a level yields its root");
    // Mark the single top page as the root and re-log its image (the flag
    // flip is covered by the new FPI).
    mark_root(buffer_pool, wal_writer, root.page_id, tree_level)?;

    let mut index = BTreeIndex::from_parts(
        Arc::clone(buffer_pool),
        Arc::clone(wal_writer),
        rel_oid,
        meta_page,
        root.page_id,
        tree_level,
        key_type,
    );
    // Last step: publish the root (module docs, "meta record is written last").
    index.write_meta_record()?;
    Ok(index)
}

/// Partition `items` into page-sized runs (by `entry_len`) and materialize
/// one page per run through `fill(page_id, prev, next, run)`, which
/// initializes the page, links it, logs its image, and returns the page's
/// subtree low key. An empty `items` produces a single empty page (the
/// root-of-empty-tree case).
fn pack_level<T>(
    buffer_pool: &Arc<BufferPool>,
    fill: impl Fn(PageId, PageId, PageId, &[T]) -> Result<Vec<u8>>,
    items: &[T],
    entry_len: impl Fn(&T) -> usize,
) -> Result<Vec<BuiltPage>> {
    // Partition into runs that each fit one page.
    let mut runs: Vec<(usize, usize)> = Vec::new();
    let mut start = 0usize;
    while start < items.len() {
        let mut used = 0usize;
        let mut end = start;
        while end < items.len() {
            let cost = entry_len(&items[end]) + 4; // entry + one line pointer
            if end > start && used + cost > USABLE_SPACE {
                break;
            }
            used += cost;
            end += 1;
        }
        runs.push((start, end));
        start = end;
    }
    if runs.is_empty() {
        runs.push((0, 0)); // empty tree: one empty root page
    }

    // Allocate all page ids of the level first: prev/next links need both
    // neighbors' ids before any page's image is logged.
    let mut page_ids = Vec::with_capacity(runs.len());
    for _ in &runs {
        let guard = buffer_pool.new_page()?;
        page_ids.push(guard.page_id());
    }

    let mut built = Vec::with_capacity(runs.len());
    for (idx, &(run_start, run_end)) in runs.iter().enumerate() {
        let prev = if idx == 0 {
            PageId::INVALID
        } else {
            page_ids[idx - 1]
        };
        let next = if idx + 1 == runs.len() {
            PageId::INVALID
        } else {
            page_ids[idx + 1]
        };
        let low_key = fill(page_ids[idx], prev, next, &items[run_start..run_end])?;
        built.push(BuiltPage {
            page_id: page_ids[idx],
            low_key,
        });
    }
    Ok(built)
}

/// Stamp `BTREE_FLAG_ROOT` on the top page and re-log its image (the flag
/// flip is covered by the new FPI).
fn mark_root(
    buffer_pool: &Arc<BufferPool>,
    wal_writer: &Arc<WalWriter>,
    page_id: PageId,
    level: u8,
) -> Result<()> {
    let mut guard = buffer_pool.pin_mut(page_id)?;
    let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().expect("frame is PAGE_SIZE");
    let flags = BtreePage::flags(page)?;
    BtreePage::set_flags(page, flags | BTREE_FLAG_ROOT);
    debug_assert_eq!(BtreePage::level(page)?, level);
    log_page_init(wal_writer, page_id, page)?;
    Ok(())
}

/// Allocate a page, initialize it as a B+Tree page (`level`/`flags`), fill
/// it with `items`, and make it durable with a post-image FPI.
fn alloc_and_log_page(
    buffer_pool: &Arc<BufferPool>,
    wal_writer: &Arc<WalWriter>,
    level: u8,
    flags: u8,
    items: &[Vec<u8>],
) -> Result<PageId> {
    let mut guard = buffer_pool.new_page()?;
    let page_id = guard.page_id();
    let page: &mut [u8; PAGE_SIZE] = guard.page_mut().try_into().expect("frame is PAGE_SIZE");
    BtreePage::init(page, level, flags);
    for item in items {
        SlottedPage::add_tuple(page, item)?;
    }
    log_page_init(wal_writer, page_id, page)?;
    Ok(page_id)
}
