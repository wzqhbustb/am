//! pg_rust catalog layer — Phase 1 M2.
//!
//! This crate implements the system catalog and relation abstraction:
//! - System tables (`pg_class`, `pg_attribute`, `pg_type`, `pg_am`, `pg_index`)
//! - Hardcoded bootstrap for empty data directories
//! - `AccessMethod` / `UpdatableAM` trait definitions
//! - `TableOid` / `TypeOid` newtype aliases
//!
//! It depends on `pg-storage` for physical types, `pg-txn` for snapshots,
//! and — since Stage H — `pg-am-heap` for the tuple codec and slotted-page
//! operations used by bootstrap (an addition to the tech-selection §一
//! dependency graph; no cycle, since `pg-am-heap` only depends on
//! `pg-storage`).

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod bootstrap;
pub mod builtin_types;
pub mod catalog;
pub mod oid;
pub mod system_tables;

use pg_storage::types::Oid;
use serde::{Deserialize, Serialize};

pub use catalog::{AmRow, AttributeRow, Catalog, RelationRow, TypeRow};
pub use oid::OidAllocator;

/// Result type used by catalog and access method operations.
pub type Result<T> = std::result::Result<T, CatalogError>;

/// Errors that can occur in catalog and access method operations.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    /// The operation is not yet implemented (Stage A skeleton).
    #[error("not implemented")]
    NotImplemented,

    /// A storage engine operation failed.
    #[error("storage error: {0}")]
    Storage(#[from] pg_storage::error::StorageError),

    /// A heap tuple or slotted-page operation failed.
    #[error("heap error: {0}")]
    Heap(#[from] pg_am_heap::HeapError),

    /// Catalog content read back from disk is malformed (wrong column type,
    /// negative OID, etc.). Distinct from [`CatalogError::Heap`], which
    /// covers malformed *encoding*; this is malformed *content*.
    #[error("corrupted catalog: {0}")]
    Corrupted(String),
}

/// A table (relation) object identifier.
///
/// This is a newtype wrapper around [`Oid`] to prevent mixing table OIDs
/// with type OIDs or other object identifiers at the type level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TableOid(pub Oid);

impl TableOid {
    /// The invalid table OID.
    pub const INVALID: TableOid = TableOid(Oid::INVALID);

    /// Wrap a raw [`Oid`] as a table OID.
    pub fn new(oid: Oid) -> Self {
        Self(oid)
    }

    /// Return the underlying raw [`Oid`].
    pub fn raw(self) -> Oid {
        self.0
    }
}

impl From<Oid> for TableOid {
    fn from(oid: Oid) -> Self {
        Self(oid)
    }
}

impl From<TableOid> for Oid {
    fn from(t: TableOid) -> Self {
        t.0
    }
}

/// A type object identifier.
///
/// This is a newtype wrapper around [`Oid`] to prevent mixing type OIDs
/// with table OIDs or other object identifiers at the type level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TypeOid(pub Oid);

impl TypeOid {
    /// The invalid type OID.
    pub const INVALID: TypeOid = TypeOid(Oid::INVALID);

    /// Wrap a raw [`Oid`] as a type OID.
    pub fn new(oid: Oid) -> Self {
        Self(oid)
    }

    /// Return the underlying raw [`Oid`].
    pub fn raw(self) -> Oid {
        self.0
    }
}

impl From<Oid> for TypeOid {
    fn from(oid: Oid) -> Self {
        Self(oid)
    }
}

impl From<TypeOid> for Oid {
    fn from(t: TypeOid) -> Self {
        t.0
    }
}

// ---------------------------------------------------------------------------
// Access Method traits
// ---------------------------------------------------------------------------
//
// Stage I moved these traits (and their operation contexts) down into
// `pg-am-heap`, so that `HeapAM` and its impls live beside the tuple /
// slotted-page primitives they build on. They are re-exported here unchanged
// to keep the catalog's public API stable — existing `pg_catalog::AccessMethod`
// paths keep working.
pub use pg_am_heap::access_method::{
    AccessMethod, BuildContext, DeleteContext, InsertContext, RelationDesc, ScanContext,
    UpdatableAM, UpdateContext, Vacuumable,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_oid_round_trip() {
        let raw = Oid(1259);
        let table = TableOid::new(raw);
        assert_eq!(table.raw(), raw);
        let back: Oid = table.into();
        assert_eq!(back, raw);
    }

    #[test]
    fn type_oid_round_trip() {
        let raw = Oid(23);
        let ty = TypeOid::new(raw);
        assert_eq!(ty.raw(), raw);
        let back: Oid = ty.into();
        assert_eq!(back, raw);
    }

    #[test]
    fn table_and_type_oid_are_distinct() {
        let table = TableOid::new(Oid(1259));
        let ty = TypeOid::new(Oid(1259));
        // Same underlying Oid, different newtypes — compile-time distinction.
        assert_eq!(table.raw(), ty.raw());
    }

    #[test]
    fn table_oid_serde_round_trip() {
        let table = TableOid::new(Oid(1259));
        let encoded = bincode::serde::encode_to_vec(table, bincode::config::standard()).unwrap();
        let (decoded, _): (TableOid, usize) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        assert_eq!(table, decoded);
    }

    #[test]
    fn type_oid_serde_round_trip() {
        let ty = TypeOid::new(Oid(23));
        let encoded = bincode::serde::encode_to_vec(ty, bincode::config::standard()).unwrap();
        let (decoded, _): (TypeOid, usize) =
            bincode::serde::decode_from_slice(&encoded, bincode::config::standard()).unwrap();
        assert_eq!(ty, decoded);
    }
}
