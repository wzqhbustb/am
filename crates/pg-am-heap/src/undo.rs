//! Heap undo handler (Stage S, §11.3).
//!
//! Heap MVCC needs no per-tuple undo: aborted transactions are invisible
//! because `xmin` is not `Committed` in the CLOG. The only undo action is
//! stamping each ATT member as `Aborted` in the CLOG so subsequent visibility
//! checks see them as aborted without scanning the WAL for `TxnAbort`
//! records that may not exist (the crash prevented the abort record from
//! being written).
//!
//! # Durability of the `Aborted` stamps (post-Stage-S review B3)
//!
//! The stamps are `set_state` calls with no WAL record and no explicit
//! flush: they are **in-memory only until the first checkpoint**, whose
//! `ClogFlush` hook (`CheckpointCoordinator::set_clog_flush`) persists the
//! dirty CLOG frames between `CheckpointBegin` and `CheckpointEnd`. A crash
//! in that window loses the stamps — harmlessly: the next recovery re-derives
//! the same ATT from the WAL (the crashed XIDs still have no terminal
//! record) and re-stamps them, and the stamps are idempotent. This matches
//! the Stage N "no explicit heap undo" decision (see `pg-storage`'s
//! `analysis` module docs): an XID with no CLOG entry reads `InProgress`,
//! which is already MVCC-invisible, so the stamp is only ever an
//! optimization, never the sole carrier of abort truth.

use pg_storage::clog::TxnState;
use pg_storage::error::Result;
use pg_storage::recovery::{UndoContext, UndoHandler};

/// Stamps every ATT XID as `Aborted` in the CLOG. The stamps reach disk via
/// the next checkpoint's CLOG flush, not via any WAL record of their own —
/// see the module docs for why losing them to a crash before that checkpoint
/// is harmless.
pub struct HeapUndoHandler;

impl UndoHandler for HeapUndoHandler {
    fn undo(&self, ctx: &mut UndoContext<'_>) -> Result<()> {
        for xid in ctx.att {
            ctx.clog.set_state(*xid, TxnState::Aborted);
        }
        Ok(())
    }
}
