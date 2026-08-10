//! Heap undo handler (Stage S, §11.3).
//!
//! Heap MVCC needs no per-tuple undo: aborted transactions are invisible
//! because `xmin` is not `Committed` in the CLOG. The only undo action is
//! stamping each ATT member as `Aborted` in the CLOG so subsequent visibility
//! checks see them as aborted without scanning the WAL for `TxnAbort`
//! records that may not exist (the crash prevented the abort record from
//! being written).

use pg_storage::clog::TxnState;
use pg_storage::error::Result;
use pg_storage::recovery::{UndoContext, UndoHandler};

/// Stamps every ATT XID as `Aborted` in the CLOG.
pub struct HeapUndoHandler;

impl UndoHandler for HeapUndoHandler {
    fn undo(&self, ctx: &mut UndoContext<'_>) -> Result<()> {
        for xid in ctx.att {
            ctx.clog.set_state(*xid, TxnState::Aborted);
        }
        Ok(())
    }
}
