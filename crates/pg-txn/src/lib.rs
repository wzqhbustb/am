//! pg_rust transaction layer — Phase 1 M2.
//!
//! This crate implements transaction management, MVCC visibility, and locking:
//! - XID allocation (`TxnIdClock`)
//! - CLOG (transaction status log) with `ClogBuffer` SLRU cache
//! - Snapshot and `VisibilityOracle`
//! - Lock Manager (row-level via tuple.xmax + table-level 4-mode locks)
//!
//! It depends only on `pg-storage` for physical types and primitives.
//!
//! # M2a scope (Stage I–J)
//!
//! Stage I added the minimal [`Snapshot`] + [`is_visible`] surface for heap
//! scan. Stage J adds the [`manager::TxnManager`] (XID allocation + durable
//! commit/abort), the [`clog_mem::InMemoryClogAccessor`] (a real CLOG that
//! records aborts), and the [`redo`] handlers that rebuild the CLOG from the
//! WAL on recovery.
//!
//! # M2b scope (Stage K–L)
//!
//! Stage K/L add the disk-backed CLOG: [`clog_file`] segment files and the
//! [`ClogBuffer`] SLRU cache, which implements the same [`ClogAccessor`] trait
//! (and the checkpoint flush hook `pg_storage::clog::ClogFlush`) so call sites
//! do not change. Stage L also completes the MVCC surface: [`Snapshot`] gains
//! its full §7.1 field set (`xip: SmallVec<[TxnId; 32]>`, `curcid`),
//! [`TxnManager::snapshot`] produces real SI snapshots, and
//! [`visibility::PgVisibilityOracle`] implements the complete §7.2 textbook
//! judgment including the `t_cid`/`curcid` self-command branches. The lock
//! manager arrives later.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod clog_buffer;
pub mod clog_file;
pub mod clog_mem;
pub mod manager;
pub mod redo;
pub mod snapshot;
pub mod visibility;

pub use clog_buffer::ClogBuffer;
pub use clog_mem::InMemoryClogAccessor;
pub use manager::{CommitWal, TxnManager};
pub use pg_storage::clog::{ClogAccessor, TxnState};
pub use redo::txn_redo_handlers;
pub use snapshot::Snapshot;
pub use visibility::{is_visible, HintBit, PgVisibilityOracle, Visibility, VisibilityOracle};
