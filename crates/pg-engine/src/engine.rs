//! The assembled engine and its programmatic API (coding-plan Stage K;
//! tech-selection §21 "M2a API").
//!
//! [`Engine`] wires the M2a components together at open time, in this order:
//!
//! 1. `StorageEngine::open_with_redo_and_clog` — storage recovery with the
//!    heap AM's and the txn layer's redo handlers injected
//!    (`heap_redo_handlers` + `txn_redo_handlers`) and a shared
//!    [`TrackingClog`] (a recording wrapper over `InMemoryClogAccessor`)
//!    that both recovery and post-recovery visibility checks read.
//!    `checkpoint.set_next_txn_id_source` is already done by `pg-storage`
//!    itself (see `StorageEngine::create_new` / `recover_with_redo_handlers`).
//! 2. The checkpoint-time **CLOG snapshot** is loaded into the CLOG (see the
//!    [`crate::clog_snapshot`] module: recovery replay alone cannot rebuild
//!    commit records from before the redo point).
//! 3. `Catalog::open` — bootstrap (if needed) + read-back of the system
//!    tables; owns the `OidAllocator` wired into checkpoints. The catalog is
//!    opened **once per Engine** and kept: re-opening after DDL would reset
//!    the OID allocator and reopen the crash-rollback window the startup
//!    correction in `Catalog::open` exists to close.
//! 4. `HeapAM::new` over the shared buffer pool / WAL writer.
//! 5. `TxnManager::new` over `engine.txn_id_clock()`, the WAL writer as
//!    `Arc<dyn CommitWal>`, and the same CLOG.
//! 6. The in-memory **table registry** (`RwLock<HashMap<String, TableEntry>>`)
//!    is rebuilt from the catalog: `pg_class` rows (last version per OID
//!    wins, `relkind = 'd'` marks a drop) joined with `pg_attribute` and
//!    `pg_rust_relpages`.
//!
//! # DDL crash atomicity (M2a limitation)
//!
//! Heap records are replayed unconditionally and there is no undo in M2a, so
//! a crash mid-DDL can leave a physical half-state. The ordering inside
//! `create_table` / `drop_table` is chosen so every half-state degrades to a
//! *leak*, never to corruption:
//!
//! - `create_table`: a crash can leave catalog rows for a table whose
//!   commit never landed. Registry rebuild skips entries without a complete
//!   `pg_class` + `pg_rust_relpages` pair (warn + leak the heap pages).
//! - `drop_table`: the `relkind = 'd'` version is written **before** the
//!   data pages are freed. A crash then leaks the pages of a table that
//!   reads as dropped; the reverse order could return a freed page to the
//!   allocator while the table still reads as live — a reused page under a
//!   live chain head is corruption, so this order is never used.
//!
//! # System-catalog capacity (M2a limitation)
//!
//! `Catalog::open` reads back only each system table's *first* page, so DDL
//! must never let `pg_class` / `pg_attribute` / `pg_rust_relpages` overflow
//! onto a second chain page — the overflow rows would vanish after a reopen.
//! Every catalog insert/update pre-checks the first page's free space and
//! fails with [`EngineError::CatalogFull`] instead of silently overflowing
//! (M2a expects the single page to be ample: a `pg_class` row is ~100
//! bytes).
//!
//! # Concurrency scope
//!
//! DML (`insert` / `scan` / `update` / `delete`) is safe to call from many
//! threads concurrently (Stage K 100-thread acceptance). DDL is serialized
//! by an internal lock, but DDL racing DML on the same table is **not**
//! supported in M2a (no table locks yet — those arrive in M2c).

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use pg_am_heap::access_method::{
    DeleteContext, InsertContext, RelationDesc, ScanContext, UpdateContext,
};
use pg_am_heap::line_pointer::LINE_POINTER_SIZE;
use pg_am_heap::tuple::{decode_tuple, encode_tuple, ColumnType, Datum, TupleHeader};
use pg_am_heap::{heap_redo_handlers, AccessMethod, HeapAM, SlottedPage, UpdatableAM};
use pg_catalog::builtin_types::{builtin_type, BUILTIN_TYPES};
use pg_catalog::system_tables::{
    SystemTableDef, HEAP_AM_OID, PG_ATTRIBUTE, PG_CLASS, PG_RELPAGES, RELKIND_TABLE,
};
use pg_catalog::{Catalog, RelationRow, TypeOid};
use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::types::{Oid, PageId, Tid, TxnId, PAGE_SIZE};
use pg_txn::{txn_redo_handlers, ClogAccessor, CommitWal, Snapshot, TxnManager};

use crate::clog_snapshot::{load_clog_snapshot, write_clog_snapshot, TrackingClog};
use crate::error::{EngineError, Result};

/// `pg_class.relkind` marker written by [`Engine::drop_table`].
///
/// Engine-private (PostgreSQL has no such relkind; it removes the row). The
/// row is kept — with its `relkind` flipped — so the drop is an ordinary
/// MVCC update through the heap AM (WAL-logged, redo-safe) instead of a
/// physical removal the M2a catalog read-back could not distinguish from
/// corruption.
const RELKIND_DROPPED: &str = "d";

/// Engine-level configuration (M2a).
///
/// A thin wrapper over [`StorageConfig`]: M2a adds no engine-specific knobs
/// yet, but wrapping keeps [`Engine::open`]'s signature stable when later
/// milestones add their own (e.g. M2b's `clog_buffer_frames`, v2.3-25)
/// without breaking callers.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Storage-layer configuration (buffer pool, WAL, checkpoints).
    pub storage: StorageConfig,
}

impl EngineConfig {
    /// Default configuration rooted at `data_dir`.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            storage: StorageConfig::new(data_dir),
        }
    }
}

/// A column definition supplied to [`Engine::create_table`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    /// Column name (`pg_attribute.attname`).
    pub name: String,
    /// The heap tuple codec type; mapped to a built-in `pg_type` OID on DDL.
    pub col_type: ColumnType,
}

/// A single column value supplied to / returned by the DML API.
/// `None` encodes SQL NULL. This is exactly the heap codec's
/// `Option<Datum>` — re-exported, not re-wrapped, so no conversion layer
/// exists between the engine API and the AM.
pub type Value = Option<Datum>;

/// A scan filter (§21 M2a minimum: single-column equality).
///
/// `None` on [`Engine::scan`] means a full scan.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// Keep rows whose column `col_index` equals `value`.
    Eq {
        /// 0-based column position in the table's schema.
        col_index: usize,
        /// The value to compare against (type must match the column).
        value: Datum,
    },
}

/// One entry of the engine's in-memory table registry.
#[derive(Debug, Clone)]
pub struct TableEntry {
    /// The table's OID (`pg_class.oid`).
    pub oid: Oid,
    /// Head of the table's on-disk page chain (`pg_rust_relpages.first_page`).
    pub first_page: PageId,
    /// Column schema in `attnum` order.
    pub columns: Vec<ColumnDef>,
}

/// The assembled M2a engine: storage + catalog + heap AM + txn manager +
/// in-memory table registry. Assembly order is documented at the module
/// level.
///
/// All operations take `&self`; every field is internally synchronized, so a
/// single `Engine` (typically behind an `Arc`) is shared across threads.
pub struct Engine {
    storage: StorageEngine,
    catalog: Catalog,
    heap: HeapAM,
    txn: TxnManager,
    clog: Arc<TrackingClog>,
    /// Name → table. Rebuilt from the catalog at open; kept in sync by DDL.
    registry: RwLock<HashMap<String, TableEntry>>,
    /// Serializes `create_table` / `drop_table` (see the module docs for the
    /// DDL-vs-DML concurrency scope).
    ddl_lock: Mutex<()>,
    /// Commit/checkpoint barrier (fixes the dump→truncate window): every
    /// auto-commit statement holds a read guard for its whole lifetime, and
    /// [`Engine::checkpoint`] holds the write guard across the CLOG snapshot
    /// dump *and* the storage checkpoint. A commit that is durable before the
    /// checkpoint's `begin_lsn` has therefore finished `clog.set_state`
    /// before the dump reads it (so it is in the snapshot); any commit that
    /// starts after the dump is assigned an LSN past `begin_lsn` (so WAL
    /// replay rebuilds it). Without the barrier a commit could append before
    /// `begin_lsn` but set its CLOG state after the dump — present in
    /// neither snapshot nor replay, i.e. invisible after restart.
    commit_barrier: RwLock<()>,
}

impl Engine {
    /// Open (or create) a database at `data_dir`, assembling all M2a layers.
    ///
    /// See the module docs for the assembly order and the redo-handler /
    /// CLOG wiring.
    pub fn open(data_dir: &Path, config: EngineConfig) -> Result<Self> {
        let clog = Arc::new(TrackingClog::new());
        let mut redo_handlers = heap_redo_handlers();
        redo_handlers.extend(txn_redo_handlers());
        let storage = StorageEngine::open_with_redo_and_clog(
            data_dir,
            &config.storage,
            redo_handlers,
            Arc::clone(&clog) as Arc<dyn ClogAccessor>,
        )?;

        // Recovery replay rebuilt the CLOG for everything after the last
        // checkpoint; the checkpoint-time snapshot (see `clog_snapshot`)
        // restores everything before it. Loading goes through `set_state`,
        // so the tracker absorbs the snapshot into its full-history dump.
        for (xid, state) in load_clog_snapshot(data_dir)? {
            clog.set_state(xid, state);
        }

        // One catalog per engine (module docs: OID-allocator monotonicity).
        let catalog = Catalog::open(&storage)?;

        let heap = HeapAM::new(
            Arc::clone(storage.buffer_pool()),
            Arc::clone(storage.wal_writer()),
        );
        let wal: Arc<dyn CommitWal> = Arc::clone(storage.wal_writer()) as Arc<dyn CommitWal>;
        let txn = TxnManager::new(
            storage.txn_id_clock(),
            wal,
            Arc::clone(&clog) as Arc<dyn ClogAccessor>,
        );

        let registry = Self::build_registry(&catalog)?;

        Ok(Self {
            storage,
            catalog,
            heap,
            txn,
            clog,
            registry: RwLock::new(registry),
            ddl_lock: Mutex::new(()),
            commit_barrier: RwLock::new(()),
        })
    }

    /// Rebuild the table registry from the catalog snapshot.
    ///
    /// `pg_class` rows are folded **last-version-wins per OID** in slot
    /// order: `drop_table` appends a `relkind = 'd'` version after the live
    /// one, and a fresh insert always appends, so the last row for an OID is
    /// its current state. System catalogs (OID < `FIRST_USER`) are not user
    /// tables and are skipped.
    fn build_registry(catalog: &Catalog) -> Result<HashMap<String, TableEntry>> {
        let mut live: HashMap<u64, &RelationRow> = HashMap::new();
        for row in catalog.relations() {
            if row.kind == RELKIND_DROPPED {
                live.remove(&row.oid.raw().0);
            } else {
                live.insert(row.oid.raw().0, row);
            }
        }

        let mut registry = HashMap::new();
        for row in live.into_values() {
            let oid = row.oid.raw();
            if oid.0 < Oid::FIRST_USER.0 {
                continue;
            }
            let Some(relpages) = catalog.relpages_of(row.oid) else {
                // Half-created table from a crash mid-create_table (module
                // docs, "DDL crash atomicity"): leak the pages, skip the row.
                tracing::warn!(
                    table = %row.name,
                    oid = oid.0,
                    "pg_class row without pg_rust_relpages entry; skipping half-created table"
                );
                continue;
            };
            let mut columns = Vec::with_capacity(row.natts as usize);
            for attr in catalog.attributes_of(row.oid) {
                let ty = builtin_type(attr.type_oid).ok_or_else(|| {
                    EngineError::Corrupted(format!(
                        "{}.{}: unknown type OID {:?}",
                        row.name, attr.name, attr.type_oid
                    ))
                })?;
                columns.push(ColumnDef {
                    name: attr.name.clone(),
                    col_type: ty.column_type,
                });
            }
            if registry
                .insert(
                    row.name.clone(),
                    TableEntry {
                        oid,
                        first_page: relpages.first_page,
                        columns,
                    },
                )
                .is_some()
            {
                return Err(EngineError::Corrupted(format!(
                    "duplicate live pg_class rows for table {:?}",
                    row.name
                )));
            }
        }
        Ok(registry)
    }

    /// Flush all dirty pages and persist the superblock (XID clock, OID
    /// counter, checkpoint LSN).
    ///
    /// First dumps the CLOG snapshot (`clog_snapshot` module: recovery
    /// replays only from the checkpoint redo point, so without the snapshot
    /// every commit record before it would be lost), **then** triggers the
    /// storage checkpoint that may truncate the WAL prefix. This is the only
    /// supported checkpoint path in M2a — background checkpointing is never
    /// started, precisely because it would bypass the dump.
    pub fn checkpoint(&self) -> Result<()> {
        // Take the commit barrier across BOTH the dump and the storage
        // checkpoint (see the field docs): in-flight commits must finish
        // `clog.set_state` before the dump, and new commits must land past
        // the checkpoint's begin_lsn. The write guard also serializes
        // concurrent `Engine::checkpoint` calls (they would otherwise race
        // on the shared `clog-snapshot.tmp` scratch file).
        let _barrier = self.commit_barrier.write();
        write_clog_snapshot(self.storage.data_dir(), &self.clog.terminal_entries())?;
        self.storage.trigger_checkpoint()?;
        Ok(())
    }

    /// Gracefully shut down background threads (checkpointer, WAL writer).
    pub fn shutdown(&self) {
        self.storage.shutdown();
    }

    /// Create a table and register it in the system catalog.
    ///
    /// Runs as one auto-commit transaction: allocate an OID, allocate the
    /// first heap page, insert the `pg_class` / `pg_attribute` /
    /// `pg_rust_relpages` rows **through the heap AM** (WAL-logged, so the
    /// catalog changes are redo-recoverable), commit, then update the
    /// in-memory registry.
    ///
    /// Fails with [`EngineError::TableExists`] if the name is taken, and
    /// with [`EngineError::CatalogFull`] if a system catalog's first page
    /// has no room (module-level M2a limitation).
    pub fn create_table(&self, name: &str, schema: &[ColumnDef]) -> Result<Oid> {
        if name.is_empty() {
            return Err(EngineError::InvalidArgument(
                "table name must not be empty".to_string(),
            ));
        }
        if schema.is_empty() {
            return Err(EngineError::InvalidArgument(
                "cannot create a table with no columns".to_string(),
            ));
        }
        let _ddl = self.ddl_lock.lock();
        if self.registry.read().contains_key(name) {
            return Err(EngineError::TableExists(name.to_string()));
        }

        let oid = self.catalog.oid_allocator().alloc();
        let first_page = self.auto_commit(|snap| self.create_table_inner(snap, oid, name, schema));
        match first_page {
            Ok(first_page) => {
                self.registry.write().insert(
                    name.to_string(),
                    TableEntry {
                        oid,
                        first_page,
                        columns: schema.to_vec(),
                    },
                );
                Ok(oid)
            }
            Err(e) => {
                // The heap page may have been allocated and tracked before
                // the failure; forget it so the AM cache cannot hand the
                // (never-committed) page to a later relation of this OID.
                self.heap.drop_relation(oid);
                Err(e)
            }
        }
    }

    /// The catalog-writing half of `create_table`, inside transaction `snap`.
    fn create_table_inner(
        &self,
        snap: &Snapshot,
        oid: Oid,
        name: &str,
        schema: &[ColumnDef],
    ) -> Result<PageId> {
        let first_page = self.heap.create_heap(oid)?;

        let class_row = vec![
            Some(Datum::Int8(oid.0 as i64)),
            Some(Datum::Text(name.to_string())),
            Some(Datum::Text(RELKIND_TABLE.to_string())),
            Some(Datum::Int4(schema.len() as i32)),
            // No TOAST table (M2a).
            Some(Datum::Int8(0)),
            Some(Datum::Int8(HEAP_AM_OID.0 as i64)),
        ];
        self.insert_catalog_row(snap, &PG_CLASS, &class_row)?;

        for (i, col) in schema.iter().enumerate() {
            let (type_oid, len) = type_oid_of(col.col_type)?;
            let attr_row = vec![
                Some(Datum::Int8(oid.0 as i64)),
                Some(Datum::Text(col.name.clone())),
                Some(Datum::Int8(type_oid.raw().0 as i64)),
                Some(Datum::Int4(len)),
                // attnum is 1-based.
                Some(Datum::Int4(i as i32 + 1)),
                // M2a ColumnDef has no nullability: nullable, not-null = 0.
                Some(Datum::Int4(0)),
                Some(Datum::Int4(1)),
            ];
            self.insert_catalog_row(snap, &PG_ATTRIBUTE, &attr_row)?;
        }

        // The chain currently has exactly one page; `last_page` /
        // `page_count` are advisory only (they go stale as the chain grows)
        // — the on-disk chain walk is authoritative (Stage K wave 1).
        let relpages_row = vec![
            Some(Datum::Int8(oid.0 as i64)),
            Some(Datum::Int8(first_page.0 as i64)),
            Some(Datum::Int8(first_page.0 as i64)),
            Some(Datum::Int8(1)),
        ];
        self.insert_catalog_row(snap, &PG_RELPAGES, &relpages_row)?;

        Ok(first_page)
    }

    /// Drop a table: mark its `pg_class` row `relkind = 'd'`, free its data
    /// pages, and remove it from the registry.
    ///
    /// The marker is an MVCC **update through the heap AM**, not a physical
    /// row removal: the old version stays in the page (invisible to scans
    /// once the drop commits), and the registry rebuild's last-version-wins
    /// fold reads the drop. A physical removal would need either slot
    /// recycling (breaks TID stability) or a raw in-place rewrite that
    /// reaches past the AM into tuple internals — both worse than one extra
    /// dead version per dropped table in M2a.
    ///
    /// Ordering inside the transaction (module docs, "DDL crash
    /// atomicity"): the `'d'` marker is written **first**, then the data
    /// pages are walked and freed (`PageFree` WAL + redo, Stage E), then
    /// the heap AM's cached page list is dropped. The `pg_rust_relpages`
    /// row is left behind; it is keyed by the table's never-reused OID and
    /// ignored by the rebuild (leaked row, documented M2a simplification).
    ///
    /// Fails with [`EngineError::TableNotFound`] if the table does not
    /// exist.
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let _ddl = self.ddl_lock.lock();
        let entry = self
            .registry
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| EngineError::TableNotFound(name.to_string()))?;

        self.auto_commit(|snap| self.drop_table_inner(snap, name, &entry))?;
        self.registry.write().remove(name);
        Ok(())
    }

    /// The page-freeing half of `drop_table`, inside transaction `snap`.
    fn drop_table_inner(&self, snap: &Snapshot, name: &str, entry: &TableEntry) -> Result<()> {
        // 1. Mark relkind = 'd' (before freeing pages — see the fn docs).
        let old_tid = self.find_live_pg_class_row(entry.oid)?;
        let columns = PG_CLASS.column_types();
        let class_row = vec![
            Some(Datum::Int8(entry.oid.0 as i64)),
            // The name is repeated so the row stays self-describing.
            Some(Datum::Text(name.to_string())),
            Some(Datum::Text(RELKIND_DROPPED.to_string())),
            Some(Datum::Int4(entry.columns.len() as i32)),
            Some(Datum::Int8(0)),
            Some(Datum::Int8(HEAP_AM_OID.0 as i64)),
        ];
        let header = tuple_header();
        let new_tuple = encode_tuple(header, &columns, &class_row)?;
        self.ensure_catalog_room(&PG_CLASS, new_tuple.len())?;
        self.heap.update(UpdateContext {
            rel: RelationDesc {
                rel_oid: PG_CLASS.oid.raw(),
                first_page: PG_CLASS.first_page,
                columns: &columns,
            },
            snapshot: snap,
            old_tid,
            new_tuple: &new_tuple,
            out_tid: None,
            clog: self.clog.as_ref(),
        })?;

        // 2. Free the data pages along the chain.
        for page_id in self.walk_chain(entry.first_page)? {
            self.storage.page_allocator().lock().free_page(page_id)?;
        }

        // 3. Forget the relation in the AM cache: the freed page IDs can be
        //    handed out again and must never be reached through this OID.
        self.heap.drop_relation(entry.oid);
        Ok(())
    }

    /// Insert one row (single auto-commit transaction) and return its TID.
    ///
    /// `values` must match the table's schema in count and types. The
    /// tuple's `t_xmin` is stamped by the AM with the transaction's own XID
    /// (Stage K), so callers cannot mislabel the writer.
    pub fn insert(&self, table: &str, values: &[Value]) -> Result<Tid> {
        let entry = self.table_entry(table)?;
        let col_types = column_types(&entry);
        let tuple = encode_row(&entry, &col_types, values)?;
        self.auto_commit(|snap| {
            let mut out_tid = Tid {
                page_id: PageId::INVALID,
                slot_id: 0,
            };
            self.heap.insert(InsertContext {
                rel: relation_desc(&entry, &col_types),
                snapshot: snap,
                tuple: &tuple,
                out_tid: Some(&mut out_tid),
            })?;
            Ok(out_tid)
        })
    }

    /// Return every visible row of `table` as `(tid, values)`.
    ///
    /// Scans with `Snapshot::everything()` against the engine's real CLOG:
    /// committed rows are visible, aborted or in-progress writers are not.
    /// `predicate` applies a single-column equality filter after visibility.
    pub fn scan(
        &self,
        table: &str,
        predicate: Option<Predicate>,
    ) -> Result<Vec<(Tid, Vec<Value>)>> {
        let entry = self.table_entry(table)?;
        let col_types = column_types(&entry);
        if let Some(Predicate::Eq { col_index, .. }) = &predicate {
            if *col_index >= entry.columns.len() {
                return Err(EngineError::InvalidPredicate(format!(
                    "table {table:?} has {} columns, predicate references column {col_index}",
                    entry.columns.len()
                )));
            }
        }
        let mut rows = self.heap.scan(ScanContext {
            rel: relation_desc(&entry, &col_types),
            snapshot: &Snapshot::everything(),
            clog: self.clog.as_ref(),
        })?;
        if let Some(Predicate::Eq { col_index, value }) = predicate {
            rows.retain(|(_, vals)| vals.get(col_index) == Some(&Some(value.clone())));
        }
        Ok(rows)
    }

    /// Replace the row at `tid` with `values` (single auto-commit
    /// transaction) and return the new version's TID.
    pub fn update(&self, table: &str, tid: Tid, values: &[Value]) -> Result<Tid> {
        let entry = self.table_entry(table)?;
        let col_types = column_types(&entry);
        let tuple = encode_row(&entry, &col_types, values)?;
        self.auto_commit(|snap| {
            let mut out_tid = Tid {
                page_id: PageId::INVALID,
                slot_id: 0,
            };
            self.heap.update(UpdateContext {
                rel: relation_desc(&entry, &col_types),
                snapshot: snap,
                old_tid: tid,
                new_tuple: &tuple,
                out_tid: Some(&mut out_tid),
                clog: self.clog.as_ref(),
            })?;
            Ok(out_tid)
        })
    }

    /// Delete the row at `tid` (logical delete: stamps `t_xmax`; single
    /// auto-commit transaction).
    pub fn delete(&self, table: &str, tid: Tid) -> Result<()> {
        let entry = self.table_entry(table)?;
        let col_types = column_types(&entry);
        self.auto_commit(|snap| {
            self.heap.delete(DeleteContext {
                rel: relation_desc(&entry, &col_types),
                snapshot: snap,
                tid,
                clog: self.clog.as_ref(),
            })?;
            Ok(())
        })
    }

    /// Run `op` as a single auto-commit transaction (§21: M2a exposes no
    /// `begin_txn`): begin, run, commit. On error the transaction is
    /// aborted best-effort and the *original* error is returned.
    fn auto_commit<T>(&self, op: impl FnOnce(&Snapshot) -> Result<T>) -> Result<T> {
        // Read-side of the commit/checkpoint barrier (see the field docs):
        // statements run concurrently with each other, but `Engine::checkpoint`
        // waits for every in-flight statement to finish commit/abort before
        // dumping the CLOG snapshot.
        let _barrier = self.commit_barrier.read();
        let xid = self.txn.begin_txn();
        let mut snap = Snapshot::everything();
        snap.current_xid = xid;
        match op(&snap) {
            Ok(v) => {
                self.txn.commit_txn(xid)?;
                Ok(v)
            }
            Err(e) => {
                if let Err(abort_err) = self.txn.abort_txn(xid) {
                    tracing::warn!(error = %abort_err, "auto-commit abort failed");
                }
                Err(e)
            }
        }
    }

    /// Insert one row into a system catalog through the heap AM, so the
    /// write is WAL-logged and redo-recoverable (Stage K: system pages use
    /// the standard 16-byte special geometry, so the AM's chain machinery
    /// applies to them unchanged).
    fn insert_catalog_row(
        &self,
        snap: &Snapshot,
        def: &SystemTableDef,
        row: &[Value],
    ) -> Result<()> {
        let columns = def.column_types();
        let tuple = encode_tuple(tuple_header(), &columns, row)?;
        self.ensure_catalog_room(def, tuple.len())?;
        self.heap.insert(InsertContext {
            rel: RelationDesc {
                rel_oid: def.oid.raw(),
                first_page: def.first_page,
                columns: &columns,
            },
            snapshot: snap,
            tuple: &tuple,
            out_tid: None,
        })?;
        Ok(())
    }

    /// Enforce the module-level M2a limitation: catalog rows must fit on
    /// the system table's first page (the only page `Catalog::open` reads
    /// back).
    fn ensure_catalog_room(&self, def: &SystemTableDef, tuple_len: usize) -> Result<()> {
        let guard = self.storage.buffer_pool().pin(def.first_page)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        let free = SlottedPage::free_space(page);
        let needed = tuple_len + LINE_POINTER_SIZE;
        if free < needed {
            return Err(EngineError::CatalogFull(format!(
                "{}: row needs {needed} bytes but the first page has {free}",
                def.name
            )));
        }
        Ok(())
    }

    /// Locate the live (`relkind = 'r'`) `pg_class` row of `oid` by a raw
    /// slot scan of the catalog's first page.
    ///
    /// Raw, not `HeapAM::scan`: catalog rows written by bootstrap carry the
    /// synthetic bootstrap XID, which the CLOG has never heard of — a
    /// visibility-filtered AM scan would see nothing. The registry only
    /// tracks live tables, so a live row must exist.
    fn find_live_pg_class_row(&self, oid: Oid) -> Result<Tid> {
        let guard = self.storage.buffer_pool().pin(PG_CLASS.first_page)?;
        let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
        let columns = PG_CLASS.column_types();
        for slot in 0..SlottedPage::slot_count(page) as u16 {
            let Some(bytes) = SlottedPage::tuple(page, slot)? else {
                continue;
            };
            let (_header, values) = decode_tuple(bytes, &columns)?;
            let matches = matches!(&values[0], Some(Datum::Int8(v)) if *v == oid.0 as i64)
                && matches!(&values[2], Some(Datum::Text(k)) if k == RELKIND_TABLE);
            if matches {
                return Ok(Tid {
                    page_id: PG_CLASS.first_page,
                    slot_id: slot,
                });
            }
        }
        Err(EngineError::Corrupted(format!(
            "pg_class has no live row for table OID {}",
            oid.0
        )))
    }

    /// Walk the on-disk page chain from `first_page` (same rules as the
    /// heap AM's chain seeding: a fresh all-zero page ends the walk, a cycle
    /// is corruption).
    fn walk_chain(&self, first_page: PageId) -> Result<Vec<PageId>> {
        let mut pages = vec![first_page];
        let mut seen = HashSet::from([first_page]);
        loop {
            let current = *pages.last().expect("chain starts non-empty");
            let guard = self.storage.buffer_pool().pin(current)?;
            let page: &[u8; PAGE_SIZE] = guard.page().try_into().expect("frame is PAGE_SIZE");
            if SlottedPage::header(page).pd_upper == 0 {
                break;
            }
            let Some(next) = SlottedPage::next_page(page)? else {
                break;
            };
            if !seen.insert(next) {
                return Err(EngineError::Corrupted(format!(
                    "page chain cycle detected at page {next} (head {first_page})"
                )));
            }
            pages.push(next);
        }
        Ok(pages)
    }

    /// The registry entry of `table`, or [`EngineError::TableNotFound`].
    fn table_entry(&self, table: &str) -> Result<TableEntry> {
        self.registry
            .read()
            .get(table)
            .cloned()
            .ok_or_else(|| EngineError::TableNotFound(table.to_string()))
    }

    /// A cloned registry entry for `table` (testing / advanced use, e.g.
    /// building a [`RelationDesc`] to drive the heap AM directly).
    pub fn describe_table(&self, table: &str) -> Option<TableEntry> {
        self.registry.read().get(table).cloned()
    }

    /// The storage engine handle (testing / advanced use).
    pub fn storage(&self) -> &StorageEngine {
        &self.storage
    }

    /// The heap access method (testing / advanced use).
    pub fn heap(&self) -> &HeapAM {
        &self.heap
    }

    /// The transaction manager (testing / advanced use, e.g. driving an
    /// explicit abort through the engine's own CLOG).
    pub fn txn_manager(&self) -> &TxnManager {
        &self.txn
    }

    /// The engine's CLOG (testing / advanced use).
    pub fn clog(&self) -> &Arc<TrackingClog> {
        &self.clog
    }
}

/// A tuple header for engine-encoded rows: every identity field is a
/// placeholder — the AM stamps `t_xmin` with the writer's XID (Stage K),
/// `t_xmax` starts INVALID, and `t_ctid` is not maintained by the AM
/// (INVALID-ish self-reference placeholder, per the M2a contract).
fn tuple_header() -> TupleHeader {
    TupleHeader::new(
        TxnId::INVALID,
        TxnId::INVALID,
        0,
        [0; 16],
        Tid {
            page_id: PageId::INVALID,
            slot_id: 0,
        },
        0,
    )
}

/// The codec column types of a registry entry, in schema order.
fn column_types(entry: &TableEntry) -> Vec<ColumnType> {
    entry.columns.iter().map(|c| c.col_type).collect()
}

/// Build the AM's relation descriptor for a registry entry.
fn relation_desc<'a>(entry: &TableEntry, col_types: &'a [ColumnType]) -> RelationDesc<'a> {
    RelationDesc {
        rel_oid: entry.oid,
        first_page: entry.first_page,
        columns: col_types,
    }
}

/// Encode `values` as a heap tuple against the entry's schema.
fn encode_row(entry: &TableEntry, col_types: &[ColumnType], values: &[Value]) -> Result<Vec<u8>> {
    if values.len() != entry.columns.len() {
        return Err(EngineError::InvalidArgument(format!(
            "table has {} columns but {} values given",
            entry.columns.len(),
            values.len()
        )));
    }
    Ok(encode_tuple(tuple_header(), col_types, values)?)
}

/// Map a codec column type to its built-in `pg_type` OID and `attlen`
/// (§5.1; the built-in set has exactly one type per codec type).
fn type_oid_of(col_type: ColumnType) -> Result<(TypeOid, i32)> {
    BUILTIN_TYPES
        .iter()
        .find(|t| t.column_type == col_type)
        .map(|t| (t.oid, t.len))
        .ok_or_else(|| EngineError::InvalidArgument(format!("no builtin type for {col_type:?}")))
}
