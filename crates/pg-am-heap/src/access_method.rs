//! Access method traits and operation contexts (tech-selection §14).
//!
//! Moved here from `pg-catalog` in Stage I so that [`crate::heap_am::HeapAM`]
//! and its trait impls live in the same crate as the tuple / slotted-page
//! primitives they build on. `pg-catalog` re-exports these traits unchanged to
//! keep its public API stable.
//!
//! The contexts group the per-operation inputs an access method needs. Because
//! `pg-am-heap` cannot depend on `pg-catalog` (that would be a cycle), a
//! relation's physical location and column schema are supplied explicitly via
//! [`RelationDesc`] rather than resolved from the catalog.

use pg_storage::recovery::RedoHandler;
use pg_storage::types::{Oid, PageId, Tid, TxnId};
use pg_txn::Snapshot;

use crate::tuple::{ColumnType, Datum};
use crate::Result;

/// Physical + schema description of a relation, resolved by the caller.
///
/// M2a has no general relation→page map, so `first_page` plus a page count
/// tracked by the caller locates the heap. `columns` drives tuple decode.
#[derive(Debug, Clone, Copy)]
pub struct RelationDesc<'a> {
    /// The relation's OID (identity / logging).
    pub rel_oid: Oid,
    /// The relation's first heap page in the shared data file.
    pub first_page: PageId,
    /// Number of pages the relation currently occupies (`>= 1`).
    pub page_count: u64,
    /// Column schema, in `attnum` order, for tuple encode/decode.
    pub columns: &'a [ColumnType],
}

/// Inputs to [`AccessMethod::insert`].
pub struct InsertContext<'a> {
    /// The target relation.
    pub rel: RelationDesc<'a>,
    /// Snapshot of the inserting transaction (supplies `current_xid`).
    pub snapshot: &'a Snapshot,
    /// Pre-encoded tuple bytes (header + null bitmap + attributes).
    pub tuple: &'a [u8],
    /// Filled with the new tuple's TID on success (§14 P0-2).
    pub out_tid: Option<&'a mut Tid>,
}

/// Inputs to [`AccessMethod::scan`].
pub struct ScanContext<'a> {
    /// The relation to scan.
    pub rel: RelationDesc<'a>,
    /// Snapshot controlling tuple visibility.
    pub snapshot: &'a Snapshot,
}

/// Inputs to [`UpdatableAM::update`].
pub struct UpdateContext<'a> {
    /// The target relation.
    pub rel: RelationDesc<'a>,
    /// Snapshot of the updating transaction (supplies `current_xid`).
    pub snapshot: &'a Snapshot,
    /// TID of the row version being replaced.
    pub old_tid: Tid,
    /// Pre-encoded bytes of the new row version.
    pub new_tuple: &'a [u8],
    /// Filled with the new version's TID on success.
    pub out_tid: Option<&'a mut Tid>,
}

/// Inputs to [`AccessMethod::delete`].
pub struct DeleteContext<'a> {
    /// The target relation.
    pub rel: RelationDesc<'a>,
    /// Snapshot of the deleting transaction (supplies `current_xid`).
    pub snapshot: &'a Snapshot,
    /// TID of the row to delete.
    pub tid: Tid,
}

/// Inputs to [`AccessMethod::build`] (M2a placeholder).
pub struct BuildContext<'a> {
    /// The relation being built.
    pub rel: RelationDesc<'a>,
}

/// Base trait for all access methods (heap, B+Tree, future HNSW/Inverted).
///
/// Stage A defined only the identity method; Stage I adds the CRUD surface and
/// the redo-handler hook.
pub trait AccessMethod: Send + Sync {
    /// AM name, corresponds to `pg_am.amname`.
    fn name(&self) -> &'static str;

    /// Build/initialize storage for a new relation (M2a: no-op default).
    fn build(&self, _ctx: BuildContext<'_>) -> Result<()> {
        Ok(())
    }

    /// Insert a tuple, filling `ctx.out_tid` with its TID.
    fn insert(&self, ctx: InsertContext<'_>) -> Result<()>;

    /// Return every visible tuple as `(tid, decoded columns)`.
    ///
    /// M2a materializes into a `Vec` to avoid iterator/lifetime plumbing; a
    /// streaming scan is future work.
    fn scan(&self, ctx: ScanContext<'_>) -> Result<Vec<(Tid, Vec<Option<Datum>>)>>;

    /// Delete the tuple at `ctx.tid` (logical delete: sets `t_xmax`).
    fn delete(&self, ctx: DeleteContext<'_>) -> Result<()>;

    /// Redo handlers this AM contributes to the recovery registry.
    ///
    /// Returned to an upper layer for registration because `pg-storage` (which
    /// owns the registry) cannot depend on this crate.
    fn redo_handlers(&self) -> Vec<Box<dyn RedoHandler>>;
}

/// AMs that support tuple updates.
///
/// In M2 only the heap AM implements this. Index AMs (B+Tree) do not — index
/// updates are modeled as delete + insert.
pub trait UpdatableAM: AccessMethod {
    /// Update the tuple at `ctx.old_tid`, producing a new version.
    fn update(&self, ctx: UpdateContext<'_>) -> Result<()>;
}

/// AMs that support vacuum / garbage collection.
///
/// M2 only defines the interface; `scan_dead_tuples` is implemented by heap in
/// Stage I for MVCC correctness testing. `reclaim` and `notify_indexes` are
/// deferred to M3.
///
/// TODO(M3): When autovacuum is introduced, consider changing the return type
/// from `Vec<Tid>` to an iterator or callback pattern to avoid materializing
/// all dead tuples on the heap for large tables.
pub trait Vacuumable {
    /// Scan `rel` for dead tuples whose `xmax` is committed and older than
    /// `oldest_xmin`. Scoped to a single relation so callers (e.g. an M3
    /// autovacuum worker) need not filter results by OID.
    fn scan_dead_tuples(&self, rel: RelationDesc<'_>, oldest_xmin: TxnId) -> Result<Vec<Tid>>;
}
