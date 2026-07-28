//! pg_rust engine — Phase 1 M2.
//!
//! This crate is the top-level assembly that wires together:
//! - `pg-storage` (page, WAL, buffer pool, checkpoint)
//! - `pg-txn` (XID, CLOG, snapshot, visibility, locks)
//! - `pg-catalog` (system tables, bootstrap, AM traits)
//! - `pg-am-heap` / `pg-am-btree` (access methods)
//!
//! It exposes the public `Engine` API and registers all redo handlers at startup.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod clog_snapshot;
pub mod engine;
pub mod error;

pub use clog_snapshot::TrackingClog;

pub use engine::{ColumnDef, Engine, EngineConfig, Predicate, TableEntry, Value};
pub use error::{EngineError, Result};

// API surface re-exports: callers of the programmatic API (tech-selection
// §21) should not need to name the lower crates for the basic types.
pub use pg_am_heap::tuple::{ColumnType, Datum};
pub use pg_storage::types::{Oid, PageId, Tid};
