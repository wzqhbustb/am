//! In-memory commit log (M2a Stage J).
//!
//! [`InMemoryClogAccessor`] is the M2a implementation of
//! [`pg_storage::clog::ClogAccessor`]: it keeps every transaction's final
//! state in a `parking_lot::RwLock<HashMap<TxnId, TxnState>>`. It replaces the
//! M1 [`pg_storage::clog::NoOpClogAccessor`] (which answered "committed" for
//! every XID) so that abort invisibility becomes observable.
//!
//! M2b replaces this with a disk-backed SLRU (`ClogBuffer`) implementing the
//! same trait, so call sites do not change.
//!
//! # ABORTED-never-GC (v2.3-2)
//!
//! Garbage collection only removes `COMMITTED` entries strictly below a bound.
//! `ABORTED` entries are always retained: a missing CLOG entry defaults to
//! [`TxnState::InProgress`], so silently dropping an aborted XID would let a
//! stale hint bit or a re-read tuple treat it as still running rather than
//! aborted. Retaining them keeps visibility decisions authoritative. In M2a
//! GC is disabled entirely by default (`m2a_clog_never_gc`).

use std::collections::HashMap;

use parking_lot::RwLock;

use pg_storage::clog::{ClogAccessor, TxnState};
use pg_storage::types::TxnId;

/// In-memory `ClogAccessor` backed by a `RwLock<HashMap<TxnId, TxnState>>`.
///
/// Reads take a shared lock; `set_state` takes an exclusive lock. An XID with
/// no recorded entry reads as [`TxnState::InProgress`] — it has neither
/// committed nor aborted yet.
#[derive(Debug)]
pub struct InMemoryClogAccessor {
    states: RwLock<HashMap<TxnId, TxnState>>,
    /// M2a safety switch (plan `m2a_clog_never_gc`, default **true**): while
    /// set, [`Self::gc_committed_below`] is a no-op. A missing CLOG entry
    /// reads as `InProgress`, so GC of committed entries is only sound once a
    /// vacuum horizon guarantees no snapshot still needs them; M2a has no such
    /// horizon, hence never-GC by default. M2b's disk SLRU truncation flips
    /// this off explicitly.
    never_gc: bool,
}

impl Default for InMemoryClogAccessor {
    fn default() -> Self {
        Self {
            states: RwLock::new(HashMap::new()),
            never_gc: true,
        }
    }
}

impl InMemoryClogAccessor {
    /// Create an empty commit log with GC disabled (`m2a_clog_never_gc = true`).
    pub fn new() -> Self {
        Self::default()
    }

    /// Create an empty commit log with GC of committed entries enabled.
    ///
    /// Only for tests and the future M2b truncation path, which must first
    /// establish a safe horizon (no live snapshot can still consult the
    /// entries below `bound`).
    pub fn with_gc_enabled() -> Self {
        Self {
            states: RwLock::new(HashMap::new()),
            never_gc: false,
        }
    }

    /// Whether GC is disabled (the `m2a_clog_never_gc` switch).
    pub fn never_gc(&self) -> bool {
        self.never_gc
    }

    /// Number of recorded entries (test/observability helper).
    pub fn len(&self) -> usize {
        self.states.read().len()
    }

    /// Whether the commit log has no recorded entries.
    pub fn is_empty(&self) -> bool {
        self.states.read().is_empty()
    }

    /// Garbage-collect `COMMITTED` entries whose XID is strictly below `bound`.
    ///
    /// No-op returning 0 while `m2a_clog_never_gc` is set (the default).
    /// `ABORTED` entries are never removed (v2.3-2); neither are `InProgress`
    /// or `SubCommitted`. Returns the number of entries removed.
    pub fn gc_committed_below(&self, bound: TxnId) -> usize {
        if self.never_gc {
            return 0;
        }
        let mut states = self.states.write();
        let before = states.len();
        states.retain(|&xid, &mut state| !(state == TxnState::Committed && xid < bound));
        before - states.len()
    }
}

impl ClogAccessor for InMemoryClogAccessor {
    fn get_state(&self, xid: TxnId) -> TxnState {
        self.states
            .read()
            .get(&xid)
            .copied()
            .unwrap_or(TxnState::InProgress)
    }

    fn set_state(&self, xid: TxnId, state: TxnState) {
        self.states.write().insert(xid, state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_xid_reads_in_progress() {
        let clog = InMemoryClogAccessor::new();
        assert_eq!(clog.get_state(TxnId(1)), TxnState::InProgress);
    }

    #[test]
    fn set_then_get_round_trips() {
        let clog = InMemoryClogAccessor::new();
        clog.set_state(TxnId(5), TxnState::Committed);
        clog.set_state(TxnId(6), TxnState::Aborted);
        assert_eq!(clog.get_state(TxnId(5)), TxnState::Committed);
        assert_eq!(clog.get_state(TxnId(6)), TxnState::Aborted);
    }

    #[test]
    fn default_is_never_gc_and_gc_is_noop() {
        let clog = InMemoryClogAccessor::new();
        assert!(clog.never_gc(), "m2a_clog_never_gc must default to true");
        clog.set_state(TxnId(1), TxnState::Committed);
        let removed = clog.gc_committed_below(TxnId(100));
        assert_eq!(removed, 0, "GC must be a no-op while never_gc is set");
        assert_eq!(clog.get_state(TxnId(1)), TxnState::Committed);
    }

    #[test]
    fn gc_removes_committed_but_keeps_aborted() {
        let clog = InMemoryClogAccessor::with_gc_enabled();
        clog.set_state(TxnId(1), TxnState::Committed);
        clog.set_state(TxnId(2), TxnState::Aborted);
        clog.set_state(TxnId(3), TxnState::Committed);

        let removed = clog.gc_committed_below(TxnId(3));
        assert_eq!(removed, 1); // only xid 1 (committed, < 3)
        assert_eq!(clog.get_state(TxnId(1)), TxnState::InProgress); // gc'd
        assert_eq!(clog.get_state(TxnId(2)), TxnState::Aborted); // kept
        assert_eq!(clog.get_state(TxnId(3)), TxnState::Committed); // >= bound
    }

    #[test]
    fn gc_never_removes_aborted_even_below_bound() {
        let clog = InMemoryClogAccessor::with_gc_enabled();
        clog.set_state(TxnId(1), TxnState::Aborted);
        let removed = clog.gc_committed_below(TxnId(100));
        assert_eq!(removed, 0);
        assert_eq!(clog.get_state(TxnId(1)), TxnState::Aborted);
    }
}
