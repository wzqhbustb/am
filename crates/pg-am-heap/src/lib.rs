//! pg_rust heap access method — Phase 1 M2.
//!
//! This crate implements the heap AM:
//! - Slotted page layout (line pointer array + tuple data)
//! - Tuple encoding/decoding (64-byte header + null bitmap + attributes)
//! - TOAST (oversized attribute storage)
//! - Heap redo handlers (`HeapInsert`, `HeapUpdate`, `HeapDelete`)
//!
//! It implements `pg-catalog::AccessMethod + UpdatableAM + Vacuumable`.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
