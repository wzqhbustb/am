//! pg_rust transaction layer — Phase 1 M2.
//!
//! This crate implements transaction management, MVCC visibility, and locking:
//! - XID allocation (`TxnIdClock`)
//! - CLOG (transaction status log) with `ClogBuffer` SLRU cache
//! - Snapshot and `VisibilityOracle`
//! - Lock Manager (row-level via tuple.xmax + table-level 4-mode locks)
//!
//! It depends only on `pg-storage` for physical types and primitives.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
