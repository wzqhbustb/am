//! Commit-status (CLOG) access abstraction.
//!
//! The commit log records each transaction's final state (4-bit status per
//! XID). The `ClogAccessor` trait lives in `pg-storage` — rather than with
//! its implementations in `pg-txn` — because [`crate::recovery::RedoContext`]
//! holds a `&dyn ClogAccessor`; placing the trait here breaks the
//! `pg-storage ⇄ pg-txn` dependency cycle (tech-selection §11.6, v2.3-Q1).
//!
//! Implementations:
//! - [`NoOpClogAccessor`] (this module): M1 placeholder, every XID reads as
//!   committed. Wired into recovery from Stage D; formally assembled into
//!   `Engine::open` in Stage F.
//! - `pg-txn::InMemoryClogAccessor` (M2a) and `pg-txn::ClogBuffer` (M2b
//!   Stage L disk SLRU) implement the same trait with zero call-site
//!   changes. `ClogBuffer` additionally implements [`ClogFlush`] so the
//!   checkpointer can drive its single authoritative flush (§6.4).

use crate::error::Result;
use crate::types::TxnId;

/// Transaction commit status (4-bit CLOG state, PG-aligned values).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TxnState {
    /// Transaction is still in progress.
    InProgress = 0,
    /// Transaction committed.
    Committed = 1,
    /// Transaction aborted.
    Aborted = 2,
    /// Reserved for subtransactions (not used in M2).
    SubCommitted = 3,
}

/// Read/write access to the commit log.
pub trait ClogAccessor: std::fmt::Debug + Send + Sync {
    /// Return the commit status of `xid`.
    fn get_state(&self, xid: TxnId) -> TxnState;
    /// Record the commit status of `xid`.
    fn set_state(&self, xid: TxnId, state: TxnState);
}

/// Durability hook for a disk-backed commit log (M2b Stage L).
///
/// The single authoritative CLOG flush point is the checkpoint: the
/// [`crate::checkpoint::CheckpointCoordinator`] calls
/// [`flush_dirty`](ClogFlush::flush_dirty) after emitting `CheckpointBegin`
/// and before emitting `CheckpointEnd` (tech-selection §6.4, v2.3-21).
/// Nowhere else flushes the CLOG on its own initiative — the commit path
/// only updates in-memory bits and relies on the `TxnCommit`/`TxnAbort` WAL
/// records for durability.
///
/// Implemented by `pg-txn::ClogBuffer`; installed via
/// [`crate::checkpoint::CheckpointCoordinator::set_clog_flush`]. Engines
/// without a disk CLOG (M1 / M2a in-memory CLOG) never install the hook and
/// checkpoints skip the CLOG flush entirely.
pub trait ClogFlush: std::fmt::Debug + Send + Sync {
    /// Write back every dirty CLOG frame to its segment file and fsync the
    /// touched segment files, then clear the dirty flags.
    fn flush_dirty(&self) -> Result<()>;
}

/// No-op `ClogAccessor` for M1 (no transactions): every XID reads as
/// [`TxnState::Committed`] and writes are ignored.
///
/// M1 never aborts and never runs visibility checks, so a constant
/// "committed" answer is the only behavior the recovery path can observe.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoOpClogAccessor;

impl ClogAccessor for NoOpClogAccessor {
    fn get_state(&self, _xid: TxnId) -> TxnState {
        TxnState::Committed
    }

    fn set_state(&self, _xid: TxnId, _state: TxnState) {
        // No-op: M1 has no commit log to update.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn noop_clog_reads_everything_as_committed() {
        let clog = NoOpClogAccessor;
        assert_eq!(clog.get_state(TxnId(1)), TxnState::Committed);
        assert_eq!(clog.get_state(TxnId(u64::MAX)), TxnState::Committed);
    }

    #[test]
    fn noop_clog_set_state_is_ignored() {
        let clog = NoOpClogAccessor;
        clog.set_state(TxnId(7), TxnState::Aborted);
        assert_eq!(clog.get_state(TxnId(7)), TxnState::Committed);
    }
}
