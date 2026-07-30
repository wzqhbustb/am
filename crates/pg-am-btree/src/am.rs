//! `BTreeAM`: the `AccessMethod` glue for the B+Tree (tech-selection §13.4,
//! §14).
//!
//! The trait surface is heap-shaped — `scan` returns
//! `Vec<(Tid, Vec<Option<Datum>>)>` and `delete` addresses a row by `Tid`
//! alone — so the B+Tree exposes its natural API on [`BTreeIndex`]
//! ([`BTreeIndex::lookup`], [`BTreeIndex::range_scan`], [`BTreeIndex::insert`],
//! [`BTreeIndex::delete`]) and adapts the trait as follows:
//!
//! - `insert`: `ctx.tuple` is the **encoded leaf entry** (`key_bytes ++
//!   tid(10B)`, see [`crate::page::encode_leaf_entry`]) — opaque bytes to the
//!   AM, matching the trait's "AM 自行按 schema 解释" contract. `ctx.out_tid`
//!   must be `None`: an index entry's physical position is not a heap TID
//!   (§14 P0-2).
//! - `scan`: a full in-order index scan adapted to the heap shape — each row
//!   is `(entry_heap_tid, [Some(decoded_key)])`. No visibility filtering
//!   happens here (index entries carry no `xmin`/`xmax`, §14): the caller
//!   fetches the heap tuple by TID and checks visibility there.
//! - `delete`: `ctx.tid` carries only the heap TID, so the entry is found by
//!   an O(n) leaf-chain scan. This is a fallback for the trait surface; the
//!   native [`BTreeIndex::delete(key, tid)`] is the O(log n) path.
//!
//! `BTreeAM` does not implement `UpdatableAM` (§13.4: index update = delete
//! + insert).

use std::sync::Arc;

use pg_am_heap::access_method::{
    AccessMethod, DeleteContext, InsertContext, RelationDesc, ScanContext,
};
use pg_am_heap::error::HeapError;
use pg_am_heap::tuple::Datum;
use pg_am_heap::Result;
use pg_storage::buffer_pool::BufferPool;
use pg_storage::recovery::RedoHandler;
use pg_storage::types::{Oid, PageId, Tid};
use pg_storage::wal::WalWriter;

use crate::error::BTreeError;
use crate::index::BTreeIndex;
use crate::key::{decode_key, is_supported_key_type};
use crate::page::{decode_leaf_entry, LEAF_TRAILER_SIZE};
use crate::redo::btree_redo_handlers;

/// The B+Tree access method: factory for [`BTreeIndex`] handles and the
/// `AccessMethod` trait adapter.
pub struct BTreeAM {
    buffer_pool: Arc<BufferPool>,
    wal_writer: Arc<WalWriter>,
}

impl BTreeAM {
    /// Create a B+Tree AM bound to the engine's buffer pool and WAL writer.
    pub fn new(buffer_pool: Arc<BufferPool>, wal_writer: Arc<WalWriter>) -> Self {
        Self {
            buffer_pool,
            wal_writer,
        }
    }

    /// Create a new index (meta page + root leaf) for `rel_oid`.
    ///
    /// The returned handle's [`BTreeIndex::meta_page`] is the relation's
    /// `first_page`, to be stored in the catalog by the caller.
    pub fn create_index(
        &self,
        rel_oid: Oid,
        key_type: pg_am_heap::tuple::ColumnType,
    ) -> crate::Result<BTreeIndex> {
        BTreeIndex::create(
            Arc::clone(&self.buffer_pool),
            Arc::clone(&self.wal_writer),
            rel_oid,
            key_type,
        )
    }

    /// Open an existing index from its meta page (e.g. after a restart).
    pub fn open_index(
        &self,
        rel_oid: Oid,
        meta_page: PageId,
        key_type: pg_am_heap::tuple::ColumnType,
    ) -> crate::Result<BTreeIndex> {
        BTreeIndex::open(
            Arc::clone(&self.buffer_pool),
            Arc::clone(&self.wal_writer),
            rel_oid,
            meta_page,
            key_type,
        )
    }

    /// Blocking bulk load (`CREATE INDEX`): sort `entries` in full
    /// `(key, tid)` order and pack a complete tree bottom-up — leaves left
    /// to right, internal levels above them — making every page durable
    /// with a post-image `FullPageImage` and publishing the root pointer in
    /// the meta page **last**. See [`crate::bulkload`] (module is private;
    /// its docs cover the fill policy and the crash-recovery argument).
    ///
    /// `entries` are encoded leaf keys plus their heap TIDs; the caller
    /// (e.g. the engine's `CREATE INDEX` path) is responsible for the heap
    /// scan and key extraction. This is the fast path — one WAL record per
    /// page, not per entry.
    pub fn build_index(
        &self,
        rel_oid: Oid,
        key_type: pg_am_heap::tuple::ColumnType,
        entries: Vec<(Vec<u8>, Tid)>,
    ) -> crate::Result<BTreeIndex> {
        crate::bulkload::build(
            &self.buffer_pool,
            &self.wal_writer,
            rel_oid,
            key_type,
            entries,
        )
    }

    /// Build a transient index handle from a trait-level relation
    /// descriptor: `rel.first_page` is the meta page, `rel.columns[0]` the
    /// indexed column type.
    fn index_for(&self, rel: &RelationDesc<'_>) -> crate::Result<BTreeIndex> {
        let key_type = *rel.columns.first().ok_or_else(|| {
            BTreeError::InvalidArgument("btree relation needs a key column".to_string())
        })?;
        if !is_supported_key_type(key_type) {
            return Err(BTreeError::InvalidArgument(format!(
                "unsupported index key type: {key_type:?}"
            )));
        }
        self.open_index(rel.rel_oid, rel.first_page, key_type)
    }
}

impl AccessMethod for BTreeAM {
    fn name(&self) -> &'static str {
        "btree"
    }

    fn insert(&self, ctx: InsertContext<'_>) -> Result<()> {
        if ctx.out_tid.is_some() {
            return Err(HeapError::InvalidArgument(
                "btree insert does not fill out_tid (index entry positions are not heap TIDs)"
                    .to_string(),
            ));
        }
        // ctx.tuple is the encoded leaf entry: key_bytes ++ tid(10B).
        if ctx.tuple.len() < LEAF_TRAILER_SIZE {
            return Err(HeapError::InvalidArgument(format!(
                "btree insert tuple of {} bytes is shorter than the {LEAF_TRAILER_SIZE}-byte tid trailer",
                ctx.tuple.len()
            )));
        }
        let (key, tid) = decode_leaf_entry(ctx.tuple).map_err(btree_to_heap)?;
        let mut index = self.index_for(&ctx.rel).map_err(btree_to_heap)?;
        index.insert(key, tid).map_err(btree_to_heap)
    }

    fn scan(&self, ctx: ScanContext<'_>) -> Result<Vec<(Tid, Vec<Option<Datum>>)>> {
        let key_type = *ctx.rel.columns.first().ok_or_else(|| {
            HeapError::InvalidArgument("btree relation needs a key column".to_string())
        })?;
        let index = self.index_for(&ctx.rel).map_err(btree_to_heap)?;
        let entries = index.range_scan(None, None).map_err(btree_to_heap)?;
        let mut out = Vec::with_capacity(entries.len());
        for (key_bytes, tid) in entries {
            let key = decode_key(key_type, &key_bytes).map_err(btree_to_heap)?;
            out.push((tid, vec![Some(key)]));
        }
        Ok(out)
    }

    fn delete(&self, ctx: DeleteContext<'_>) -> Result<()> {
        // The trait delete carries only the heap TID: find the entry by a
        // leaf-chain scan, then remove it. O(n) fallback — the native
        // `BTreeIndex::delete(key, tid)` is the real path.
        let mut index = self.index_for(&ctx.rel).map_err(btree_to_heap)?;
        let entries = index.range_scan(None, None).map_err(btree_to_heap)?;
        for (key_bytes, tid) in entries {
            if tid == ctx.tid {
                return index.delete(&key_bytes, tid).map_err(btree_to_heap);
            }
        }
        Err(HeapError::TupleNotFound(ctx.tid))
    }

    fn redo_handlers(&self) -> Vec<Box<dyn RedoHandler>> {
        btree_redo_handlers()
    }
}

/// Map a B+Tree error onto the heap error type the `AccessMethod` trait
/// returns, preserving structure where the heap type has a matching
/// variant.
fn btree_to_heap(e: BTreeError) -> HeapError {
    match e {
        BTreeError::Storage(s) => HeapError::Storage(s),
        BTreeError::Heap(h) => h,
        BTreeError::Corrupted(m) => HeapError::Corrupted(m),
        BTreeError::PageFull { needed, available } => HeapError::PageFull { needed, available },
        other => HeapError::InvalidArgument(format!("btree: {other}")),
    }
}
