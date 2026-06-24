//! Shared transaction-ID clock (Stage J; tech-selection §5.3, coding-plan Stage J).
//!
//! Mirrors [`crate::oid::OidCounter`]: an `Arc<AtomicU64>` so the checkpoint
//! coordinator (which persists `next_txn_id` into the superblock) and the
//! `pg-txn` `TxnManager` (which allocates XIDs) can share one monotone counter
//! without exposing raw atomics across the crate boundary.
//!
//! `pg-storage` cannot depend on `pg-txn`, so the clock lives here and the
//! transaction manager drives it, exactly as the catalog drives `OidCounter`.

use std::sync::Arc;

use crate::sync::atomic::{AtomicU64, Ordering};

use crate::types::TxnId;

/// A shared, monotonically increasing 64-bit transaction-ID clock.
///
/// Clones share the same underlying counter; [`alloc`](Self::alloc) is
/// wait-free and safe to call from multiple threads concurrently. `0` is
/// reserved for [`TxnId::INVALID`], so a fresh clock starts at
/// [`TxnId::FIRST`].
#[derive(Debug, Clone)]
pub struct TxnIdClock {
    next: Arc<AtomicU64>,
}

impl TxnIdClock {
    /// Create a clock whose next [`alloc`](Self::alloc) returns `start`.
    ///
    /// `start` is normally the superblock's `next_txn_id`; a brand-new
    /// database seeds it with [`TxnId::FIRST`].
    pub fn new(start: TxnId) -> Self {
        Self {
            next: Arc::new(AtomicU64::new(start.0)),
        }
    }

    /// Allocate the next transaction ID.
    pub fn alloc(&self) -> TxnId {
        TxnId(self.next.fetch_add(1, Ordering::Relaxed))
    }

    /// Return the XID the next [`alloc`](Self::alloc) call will hand out.
    ///
    /// This is the value checkpoints persist into the superblock's
    /// `next_txn_id` field.
    pub fn current(&self) -> TxnId {
        TxnId(self.next.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_is_monotonic_and_clones_share() {
        let clock = TxnIdClock::new(TxnId::FIRST);
        let clone = clock.clone();
        assert_eq!(clock.alloc(), TxnId(1));
        assert_eq!(clone.alloc(), TxnId(2));
        assert_eq!(clock.current(), TxnId(3));
        assert_eq!(clone.current(), TxnId(3));
    }

    #[test]
    fn seeds_from_superblock_value() {
        let clock = TxnIdClock::new(TxnId(42));
        assert_eq!(clock.alloc(), TxnId(42));
        assert_eq!(clock.current(), TxnId(43));
    }
}
