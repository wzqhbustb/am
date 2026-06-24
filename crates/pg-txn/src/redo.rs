//! Transaction redo handlers (M2a Stage J).
//!
//! Two handlers replay the transaction-control WAL records emitted by
//! [`crate::manager::TxnManager`]: [`TxnCommitHandler`] and
//! [`TxnAbortHandler`]. Each decodes its payload and writes the recorded
//! terminal state into the recovery [`RedoContext`]'s commit log
//! (`ctx.clog`), so the CLOG is rebuilt authoritatively from the WAL after a
//! crash — a present `TxnCommit`/`TxnAbort` fixes the XID's state regardless
//! of any hint bits left on data pages.
//!
//! The handlers are stateless (the CLOG arrives via [`RedoContext`]) so
//! [`txn_redo_handlers`] can hand fresh boxes to the recovery registry, which
//! `pg-storage` cannot construct itself (it must not depend on this crate).
//!
//! # Idempotency
//!
//! Replay may re-run any prefix of records after a crash during recovery.
//! Both handlers are idempotent: `set_state` is a last-writer-wins map insert,
//! and a committed/aborted XID's terminal state never changes, so re-applying
//! the same record reproduces the identical CLOG entry.

use pg_storage::clog::TxnState;
use pg_storage::error::Result;
use pg_storage::recovery::{RedoContext, RedoHandler};
use pg_storage::wal::record::{TxnAbortRecord, TxnCommitRecord, WalRecord};
use pg_storage::wal::WalRecordType;

/// The transaction redo handlers, ready for injection into the recovery
/// registry before a crash-recovery replay (see
/// `StorageEngine::open_with_redo_and_clog`).
///
/// `pg-storage` owns the registry but cannot depend on this crate, so the
/// caller opening the engine must pass these in alongside the heap handlers.
pub fn txn_redo_handlers() -> Vec<Box<dyn RedoHandler>> {
    vec![Box::new(TxnCommitHandler), Box::new(TxnAbortHandler)]
}

/// Redo handler for `TxnCommit` records: marks the XID `Committed` in the CLOG.
pub struct TxnCommitHandler;

impl RedoHandler for TxnCommitHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::TxnCommit
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = TxnCommitRecord::decode(&record.payload)?;
        ctx.clog.set_state(rec.xid, TxnState::Committed);
        Ok(())
    }
}

/// Redo handler for `TxnAbort` records: marks the XID `Aborted` in the CLOG.
pub struct TxnAbortHandler;

impl RedoHandler for TxnAbortHandler {
    fn kind(&self) -> WalRecordType {
        WalRecordType::TxnAbort
    }

    fn apply(&self, record: &WalRecord, ctx: &mut RedoContext<'_>) -> Result<()> {
        let rec = TxnAbortRecord::decode(&record.payload)?;
        ctx.clog.set_state(rec.xid, TxnState::Aborted);
        Ok(())
    }
}
