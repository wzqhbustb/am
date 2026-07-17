//! pg_rust B+Tree access method — Phase 1 M2.
//!
//! This crate implements the B+Tree AM:
//! - Blink Tree variant (Lehman-Yao) with `btpo_next` for lock-free reads during splits
//! - Latch coupling for concurrent access
//! - 3-step split WAL protocol (`BTreeSplitPrepare/Copy/Commit`)
//! - Pessimistic split with restart from root
//!
//! It implements `pg-catalog::AccessMethod` (not `UpdatableAM`; index updates are
//! delete + insert).

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
