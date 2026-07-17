//! pg_rust catalog layer — Phase 1 M2.
//!
//! This crate implements the system catalog and relation abstraction:
//! - System tables (`pg_class`, `pg_attribute`, `pg_type`, `pg_am`, `pg_index`)
//! - Hardcoded bootstrap for empty data directories
//! - `AccessMethod` / `UpdatableAM` trait definitions
//! - `TableOid` / `TypeOid` newtype aliases
//!
//! It depends on `pg-storage` for physical types and `pg-txn` for snapshots.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

use pg_storage::types::{Oid, Tid, TxnId};
use serde::{Deserialize, Serialize};

/// Result type used by catalog and access method operations.
pub type Result<T> = std::result::Result<T, CatalogError>;

/// Errors that can occur in catalog and access method operations.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    /// The operation is not yet implemented (Stage A skeleton).
    #[error("not implemented")]
    NotImplemented,
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
// Access Method traits (Stage A minimal skeleton; expanded in Stage I/L)
// ---------------------------------------------------------------------------

/// Base trait for all access methods (heap, B+Tree, future HNSW/Inverted).
///
/// Stage A only defines the identity method. Additional methods (`build`,
/// `insert`, `scan`, `delete`, `redo_handlers`) are added in Stage I once
/// `RedoHandler` and Context types are available from Stage D.
pub trait AccessMethod: Send + Sync {
    /// AM name, corresponds to `pg_am.amname`.
    fn name(&self) -> &'static str;
}

/// AMs that support in-place tuple updates.
///
/// In M2 only the heap AM implements this. Index AMs (B+Tree) do not —
/// index updates are modeled as delete + insert.
///
/// Stage A is an empty marker trait; `update` is added in Stage I.
pub trait UpdatableAM: AccessMethod {}

/// AMs that support vacuum / garbage collection.
///
/// M2 only defines the interface; `scan_dead_tuples` is implemented by heap
/// in Stage I for MVCC correctness testing. `reclaim` and `notify_indexes`
/// are deferred to M3.
///
/// TODO(M3): When autovacuum is introduced, consider changing the return
/// type from `Vec<Tid>` to an iterator or callback pattern to avoid
/// materializing all dead tuples on the heap for large tables.
pub trait Vacuumable {
    /// Scan for dead tuples whose `xmax` is committed and older than
    /// `oldest_xmin`.
    fn scan_dead_tuples(&self, oldest_xmin: TxnId) -> Result<Vec<Tid>>;
}

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

    // Dummy AM to verify trait bounds compile and are implementable.
    struct DummyHeap;

    impl AccessMethod for DummyHeap {
        fn name(&self) -> &'static str {
            "heap"
        }
    }

    impl UpdatableAM for DummyHeap {}

    impl Vacuumable for DummyHeap {
        fn scan_dead_tuples(&self, _oldest_xmin: TxnId) -> Result<Vec<Tid>> {
            Ok(vec![])
        }
    }

    #[test]
    fn access_method_name() {
        let am = DummyHeap;
        assert_eq!(am.name(), "heap");
    }

    #[test]
    fn updatable_am_extends_access_method() {
        fn assert_updatable<T: UpdatableAM>(_: &T) {}
        assert_updatable(&DummyHeap);
    }

    #[test]
    fn vacuumable_scan_returns_empty() {
        let vac = DummyHeap;
        let dead = vac.scan_dead_tuples(TxnId(100)).unwrap();
        assert!(dead.is_empty());
    }
}
