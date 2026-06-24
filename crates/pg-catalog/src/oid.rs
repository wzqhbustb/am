//! OID allocation (tech-selection §5.3).
//!
//! OIDs come from a global monotonically increasing 64-bit counter (no
//! wraparound in practice, unlike PostgreSQL's 32-bit OID counter). The
//! counter is persisted in the v2 superblock's `next_oid` field on every
//! checkpoint (see `pg-storage`'s `CheckpointCoordinator::set_next_oid_source`).
//!
//! # Crash rollback window (coding-plan Stage H ⚠️)
//!
//! Until CheckpointEnd WAL records switch to v2 (Stage N), `next_oid` is
//! persisted *only* by checkpoints. A crash therefore rolls `next_oid` back
//! to the value at the last checkpoint, and OIDs allocated after that
//! checkpoint (and already written into catalog pages) could be handed out
//! again. The mitigation, per the coding plan, is startup correction:
//! [`crate::catalog::Catalog::open`] scans the system tables for the maximum
//! OID already in use and loads the allocator with
//! `max(superblock.next_oid, max_oid_in_use + 1)`. Uniqueness then rests on
//! that correction plus the allocator's monotonicity — there is deliberately
//! **no** per-allocation existence check in M2a, and no WAL record for OID
//! allocation (adding one would change the on-disk format frozen in Stage C).

use pg_storage::oid::OidCounter;
use pg_storage::types::Oid;

/// Monotonic OID allocator backed by a shared [`OidCounter`].
///
/// The counter is reference-counted so the same value can be shared with the
/// checkpoint coordinator ([`OidAllocator::shared_counter`]), which persists
/// it into the superblock on every checkpoint.
///
/// `alloc` is wait-free and safe to call from multiple threads concurrently.
#[derive(Debug, Clone)]
pub struct OidAllocator {
    /// The shared counter holding the next OID to hand out (same semantics
    /// as `Superblock::next_oid`).
    counter: OidCounter,
}

impl OidAllocator {
    /// Create an allocator whose next allocation returns `start`.
    ///
    /// Callers are expected to pass a value already corrected against the
    /// OIDs present in the catalog (see the module-level note); this type
    /// itself only guarantees monotonicity from `start`.
    pub fn load(start: Oid) -> Self {
        Self {
            counter: OidCounter::new(start),
        }
    }

    /// Allocate the next OID.
    pub fn alloc(&self) -> Oid {
        self.counter.alloc()
    }

    /// Return the OID the next [`alloc`](Self::alloc) call will hand out.
    ///
    /// This is the value persisted into the superblock's `next_oid` field by
    /// checkpoints.
    pub fn current(&self) -> Oid {
        self.counter.current()
    }

    /// Return the shared counter for wiring into
    /// `pg-storage`'s `StorageEngine::set_next_oid_source`.
    pub fn shared_counter(&self) -> OidCounter {
        self.counter.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_is_monotonic_from_start() {
        let alloc = OidAllocator::load(Oid::FIRST_USER);
        assert_eq!(alloc.current(), Oid::FIRST_USER);
        assert_eq!(alloc.alloc(), Oid(16384));
        assert_eq!(alloc.alloc(), Oid(16385));
        assert_eq!(alloc.current(), Oid(16386));
    }

    #[test]
    fn shared_counter_observes_allocations() {
        let alloc = OidAllocator::load(Oid(42));
        let counter = alloc.shared_counter();
        assert_eq!(counter.current(), Oid(42));
        alloc.alloc();
        assert_eq!(counter.current(), Oid(43));
    }
}
