//! B+Tree undo handler (Stage S, §11.3).
//!
//! Finishes incomplete B+Tree splits detected during redo. Each incomplete
//! split reached Prepare (and optionally Copy) but never Commit before the
//! crash. The handler calls [`finish_incomplete_split`] to complete the
//! split, which emits a `BTreeSplitCLR` so the result is durable.

use pg_storage::error::{Result, StorageError};
use pg_storage::recovery::{UndoContext, UndoHandler};

use crate::error::BTreeError;
use crate::index::{choose_split_slot_readonly, finish_incomplete_split, SplitToFinish};

fn btree_to_storage(e: BTreeError) -> StorageError {
    match e {
        BTreeError::Storage(s) => s,
        other => StorageError::MetadataCorrupted(format!("btree undo: {other}")),
    }
}

/// Finishes incomplete B+Tree splits after crash recovery redo.
pub struct BTreeUndoHandler;

impl UndoHandler for BTreeUndoHandler {
    fn undo(&self, ctx: &mut UndoContext<'_>) -> Result<()> {
        let splits = ctx.incomplete_splits.incomplete_splits();
        if splits.is_empty() {
            return Ok(());
        }

        // Sort by level descending: leaf splits first (parents may also be
        // incomplete, and a parent split's downlink insertion needs the
        // parent's own split finished first).
        let mut sorted: Vec<_> = splits.iter().collect();
        sorted.sort_by_key(|(_, split)| std::cmp::Reverse(split.level));

        for (left_page, split) in sorted {
            let copy_start_slot = match split.copy_start_slot {
                Some(slot) => slot,
                None => {
                    // Only Prepare was reached — compute the midpoint split slot.
                    choose_split_slot_readonly(
                        ctx.buffer_pool,
                        *left_page,
                        split.level,
                    )
                    .map_err(btree_to_storage)?
                }
            };

            finish_incomplete_split(
                ctx.buffer_pool,
                ctx.wal_writer,
                ctx.page_allocator,
                &SplitToFinish {
                    left_page: split.left_page,
                    right_page: split.right_page,
                    level: split.level,
                    copy_start_slot,
                    // The Prepare record's LSN is the redo reference point.
                    // We don't track it in the IncompleteSplit, so use INVALID
                    // — the CLR's redo_ref_lsn is only used for diagnostics.
                    prepare_lsn: pg_storage::types::Lsn::INVALID,
                },
            )
            .map_err(btree_to_storage)?;
        }
        Ok(())
    }
}
