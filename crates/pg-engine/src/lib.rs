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
