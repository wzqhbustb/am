//! pg_rust B+Tree access method — Phase 1 M2b Stage M (wave 1).
//!
//! This crate implements the B+Tree AM (tech-selection §13):
//!
//! - **Page format** ([`page`]): reuses the heap slotted page; the 16-byte
//!   special space holds the `btpo_prev`/`btpo_next` sibling pointers and
//!   `pd_flags` bits 8..11 / 12..15 carry `btpo_level` / `btpo_flags` (§13.1).
//! - **Order-preserving keys** ([`key`]): single-column keys of type
//!   `Int4`/`Int8`/`Text`/`Bytea`, encoded so byte order equals native order.
//! - **Single-threaded core** ([`index`]): [`BTreeIndex`] with point lookup,
//!   range scan, insert (with pessimistic full-path-exclusive splits) and
//!   physical delete. Blink-style right hops over `btpo_next` keep reads
//!   correct across incompletely-committed splits (§13.2).
//! - **3-step split WAL** (`BTreeSplitPrepare`/`Copy`/`Commit`, §13.3) with a
//!   minimal `Copy` payload: redo recomputes the moved entries from the left
//!   page, anchored by `left_page_pre_lsn`.
//! - **Redo handlers** ([`redo`]): stateless, idempotent, heap-style.
//! - **AM glue** ([`am`]): [`BTreeAM`] implements
//!   `pg_am_heap::access_method::AccessMethod` (not `UpdatableAM`; index
//!   updates are delete + insert, §13.4).
//!
//! M2b is single-threaded; the latch-coupling structure of the descent is a
//! skeleton for M2c concurrency (§13.2). Finishing incomplete splits during
//! undo (`BTreeSplitCLR`) is M2c work; wave 1 leaves recovered incomplete
//! splits in place and relies on the Blink right-hop read path.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod am;
mod bulkload;
pub mod error;
pub mod index;
pub mod key;
pub mod page;
pub mod redo;

pub use am::BTreeAM;
pub use error::{BTreeError, Result};
pub use index::{BTreeIndex, SplitState};
pub use key::{
    decode_i32, decode_i64, decode_key, encode_i32, encode_i64, encode_key, is_supported_key_type,
    MAX_INDEX_KEY_BYTES,
};
pub use redo::btree_redo_handlers;
