//! Shared OID counter (Stage H; tech-selection §5.3).
//!
//! Wraps an `Arc<AtomicU64>` so the checkpoint coordinator and the catalog's
//! OID allocator can share one counter without exposing raw atomics — and
//! the memory-ordering choices behind them — across the crate boundary.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::types::Oid;

/// A shared, monotonically increasing 64-bit OID counter.
///
/// Clones share the same underlying counter; `alloc` is wait-free and safe
/// to call from multiple threads concurrently.
#[derive(Debug, Clone)]
pub struct OidCounter {
    next: Arc<AtomicU64>,
}

impl OidCounter {
    /// Create a counter whose next [`alloc`](Self::alloc) returns `start`.
    pub fn new(start: Oid) -> Self {
        Self {
            next: Arc::new(AtomicU64::new(start.0)),
        }
    }

    /// Allocate the next OID.
    pub fn alloc(&self) -> Oid {
        Oid(self.next.fetch_add(1, Ordering::Relaxed))
    }

    /// Return the OID the next [`alloc`](Self::alloc) call will hand out.
    ///
    /// This is the value checkpoints persist into the superblock's
    /// `next_oid` field.
    pub fn current(&self) -> Oid {
        Oid(self.next.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alloc_is_monotonic_and_clones_share() {
        let counter = OidCounter::new(Oid::FIRST_USER);
        let clone = counter.clone();
        assert_eq!(counter.alloc(), Oid(16384));
        assert_eq!(clone.alloc(), Oid(16385));
        assert_eq!(counter.current(), Oid(16386));
        assert_eq!(clone.current(), Oid(16386));
    }
}
