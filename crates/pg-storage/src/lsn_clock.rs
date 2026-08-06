//! Monotonic LSN allocator.
//!
//! The [`LsnClock`] hands out aligned LSNs that serve as byte offsets into the
//! WAL stream. It is intended to be used by the WAL writer thread to allocate
//! LSNs; other threads can read the current LSN via [`LsnClock::current`].

use crate::sync::atomic::{AtomicU64, Ordering};

use crate::types::{Lsn, LSN_ALIGNMENT};

/// A thread-safe monotonic allocator for [`Lsn`] values.
#[derive(Debug)]
pub struct LsnClock {
    next: AtomicU64,
}

impl LsnClock {
    /// Create a clock that will hand out LSNs starting at `start`.
    ///
    /// `start` must be valid (non-zero) and a multiple of [`LSN_ALIGNMENT`].
    pub fn new(start: Lsn) -> Self {
        assert!(
            start.is_valid(),
            "LsnClock start must be a valid (non-zero) LSN"
        );
        assert!(
            start.0 % LSN_ALIGNMENT == 0,
            "LsnClock start must be aligned to {LSN_ALIGNMENT}"
        );
        Self {
            next: AtomicU64::new(start.0),
        }
    }

    /// Allocate the next contiguous chunk of `record_size` bytes in the WAL
    /// stream and return the LSN at which the record should be written.
    ///
    /// `record_size` must be a positive multiple of [`LSN_ALIGNMENT`].
    ///
    /// Only the WAL Writer thread should call this method. The `AtomicU64` is
    /// used so that [`LsnClock::current`] can be read lock-free from other
    /// threads.
    pub(crate) fn next(&self, record_size: u64) -> Lsn {
        assert!(record_size > 0, "record_size must be > 0");
        assert!(
            record_size % LSN_ALIGNMENT == 0,
            "record_size must be a multiple of {LSN_ALIGNMENT}"
        );
        // The clock only protects a single counter. The WAL writer relies on
        // fetch_add atomicity, not on ordering with other shared state, so
        // Relaxed is sufficient.
        let lsn = self.next.fetch_add(record_size, Ordering::Relaxed);
        Lsn(lsn)
    }

    /// Reserve a contiguous chunk of `record_size` bytes in the WAL stream
    /// without writing any record.
    ///
    /// The caller is responsible for emitting a record that exactly covers
    /// the reserved range afterwards (see `WalWriter::append_at`). Other
    /// threads may keep allocating from the clock in the meantime: `fetch_add`
    /// hands out non-overlapping ranges, so the reserved range remains
    /// exclusively owned by the caller.
    /// This is used by the checkpoint coordinator to pre-allocate the
    /// `CheckpointBegin` LSN so that `set_checkpoint_lsn` can be called
    /// before the record is actually written, eliminating the FPI race window.
    ///
    /// `record_size` must be a positive multiple of [`LSN_ALIGNMENT`].
    pub fn reserve(&self, record_size: u64) -> Lsn {
        assert!(record_size > 0, "record_size must be > 0");
        assert!(
            record_size % LSN_ALIGNMENT == 0,
            "record_size must be a multiple of {LSN_ALIGNMENT}"
        );
        let lsn = self.next.fetch_add(record_size, Ordering::Relaxed);
        Lsn(lsn)
    }

    /// Return the next LSN that would be handed out without advancing the
    /// clock.
    pub fn current(&self) -> Lsn {
        Lsn(self.next.load(Ordering::Relaxed))
    }
}

impl Default for LsnClock {
    fn default() -> Self {
        Self::new(Lsn::FIRST)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_at_first_lsn() {
        let clock = LsnClock::default();
        assert_eq!(clock.current(), Lsn::FIRST);
    }

    #[test]
    fn allocates_monotonic_aligned_lsns() {
        let clock = LsnClock::default();
        assert_eq!(clock.next(8), Lsn(8));
        assert_eq!(clock.next(24), Lsn(16));
        assert_eq!(clock.next(16), Lsn(40));
        assert_eq!(clock.current(), Lsn(56));
    }

    #[test]
    #[should_panic]
    fn rejects_unaligned_record_size() {
        let clock = LsnClock::default();
        let _ = clock.next(5);
    }

    #[test]
    #[should_panic]
    fn rejects_zero_record_size() {
        let clock = LsnClock::default();
        let _ = clock.next(0);
    }

    #[test]
    #[should_panic]
    fn rejects_invalid_start_lsn() {
        let _ = LsnClock::new(Lsn::INVALID);
    }

    #[test]
    fn concurrent_allocations_are_monotonic() {
        use std::sync::Arc;
        use std::thread;

        let clock = Arc::new(LsnClock::default());
        let mut handles = Vec::new();

        for _ in 0..8 {
            let c = Arc::clone(&clock);
            handles.push(thread::spawn(move || {
                let mut lsns = Vec::new();
                for _ in 0..100 {
                    lsns.push(c.next(8).0);
                }
                lsns
            }));
        }

        let mut all = Vec::new();
        for h in handles {
            all.extend(h.join().unwrap());
        }

        all.sort_unstable();
        // 800 allocations of 8 bytes each starting at 8.
        for (i, lsn) in all.iter().enumerate() {
            assert_eq!(*lsn, 8 + i as u64 * 8);
        }
    }

    #[test]
    fn reserve_allocates_without_writing() {
        let clock = LsnClock::default();
        let r1 = clock.reserve(8);
        let r2 = clock.reserve(16);
        assert_eq!(r1, Lsn(8));
        assert_eq!(r2, Lsn(16));
        assert_eq!(clock.current(), Lsn(32));

        // next() continues from where reserve() left off.
        let n1 = clock.next(8);
        assert_eq!(n1, Lsn(32));
        assert_eq!(clock.current(), Lsn(40));
    }

    #[test]
    fn reserve_and_next_are_interchangeable_for_allocation() {
        let clock = LsnClock::default();
        let _ = clock.reserve(8);
        let _ = clock.next(8);
        let _ = clock.reserve(8);
        assert_eq!(clock.current(), Lsn(32));
    }

    #[test]
    #[should_panic]
    fn reserve_rejects_unaligned_size() {
        let clock = LsnClock::default();
        let _ = clock.reserve(5);
    }

    #[test]
    #[should_panic]
    fn reserve_rejects_zero_size() {
        let clock = LsnClock::default();
        let _ = clock.reserve(0);
    }
}
