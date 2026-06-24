//! Integration test for the CheckpointEnd v1 → v2 payload migration
//! (tech-selection §11.4, v2.3-17; M2b Stage N acceptance).
//!
//! An M1 data directory holds a v1 `CheckpointEnd` (`flags = 0`, 3-field
//! payload). M2 recovery must decode it through the defaults path —
//! `next_oid = 16384`, empty `att_file`/`dpt_file` meaning "rebuild by a
//! full WAL scan from the checkpoint LSN" — without rewriting the record,
//! and the next M2-triggered checkpoint must emit the v2 format
//! (`flags = 1 << 4`, six fields) with the ATT/DPT snapshot files on disk.

use std::mem;
use std::path::Path;

use pg_storage::config::StorageConfig;
use pg_storage::engine::StorageEngine;
use pg_storage::page::PAGE_HEADER_SIZE;
use pg_storage::superblock::Superblock;
use pg_storage::types::{Lsn, Oid, PageId, TxnId};
use pg_storage::wal::reader::WalReader;
use pg_storage::wal::record::{
    CheckpointEndRecord, WalRecord, WalRecordType, CHECKPOINT_END_V2_FLAGS,
};

/// The M1 (v1) `CheckpointEnd` payload layout: three fields, no snapshot
/// files. bincode encodes a struct as its field sequence, so this produces
/// payloads byte-identical to what an M1 binary wrote.
#[derive(serde::Serialize)]
struct V1CheckpointEndPayload {
    checkpoint_lsn: Lsn,
    next_page_id: PageId,
    next_txn_id: TxnId,
}

/// Collect every `CheckpointEnd` record in the WAL at `data_dir`,
/// scanning from `from`.
fn checkpoint_end_records(data_dir: &Path, segment_size: u64, from: Lsn) -> Vec<WalRecord> {
    let mut reader = WalReader::open_at(data_dir.join("wal"), segment_size, from).unwrap();
    let mut out = Vec::new();
    while let Some(rec) = reader.next_record().unwrap() {
        if rec.record_type == WalRecordType::CheckpointEnd {
            out.push(rec);
        }
    }
    out
}

#[test]
fn test_checkpoint_v1_v2_migration() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut config = StorageConfig::new(tmp.path());
    config.wal_group_commit_timeout_ms = 1;
    config.wal_group_commit_batch_size = 1;

    let v1_checkpoint_lsn;
    let page_id;

    // -- Phase 1: an M1 binary leaves a v1 CheckpointEnd behind -----------
    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();

        // Dirty a page and flush it, as an M1 checkpoint's flush phase
        // would have: the content must be on the data pages, because the
        // records that produced it predate the redo point.
        {
            let mut guard = engine.buffer_pool().new_page().unwrap();
            page_id = guard.page_id();
            guard.page_mut()[PAGE_HEADER_SIZE] = 0x77;
        }
        engine.buffer_pool().flush(page_id).unwrap();

        // M1 checkpoint: CheckpointBegin + a *v1* CheckpointEnd (flags = 0,
        // 3-field payload), flush, then the superblock update.
        let begin_lsn = engine
            .wal_writer()
            .append(WalRecord::checkpoint_begin())
            .unwrap();
        let payload = bincode::serde::encode_to_vec(
            V1CheckpointEndPayload {
                checkpoint_lsn: begin_lsn,
                next_page_id: engine.page_allocator().lock().next_page_id(),
                next_txn_id: engine.superblock().lock().next_txn_id,
            },
            bincode::config::standard(),
        )
        .unwrap();
        let v1_end = WalRecord {
            lsn: Lsn::INVALID,
            prev_lsn: Lsn::INVALID,
            txn_id: TxnId::INVALID,
            record_type: WalRecordType::CheckpointEnd,
            flags: 0,
            payload,
        };
        let end_lsn = engine.wal_writer().append(v1_end).unwrap();
        engine.wal_writer().flush_to(end_lsn).unwrap();
        {
            let mut sb = engine.superblock().lock();
            sb.checkpoint_lsn = begin_lsn;
            sb.next_page_id = engine.page_allocator().lock().next_page_id();
            sb.write(&Superblock::path(tmp.path())).unwrap();
        }
        v1_checkpoint_lsn = begin_lsn;

        mem::forget(engine); // kill -9
    }

    // -- Phase 2: M2 opens the M1 directory (v1 defaults path) ------------
    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();

        // Recovery succeeded via the v1 defaults: empty att_file → full
        // rebuild from the checkpoint LSN.
        let guard = engine.buffer_pool().pin(page_id).unwrap();
        assert_eq!(guard.page()[PAGE_HEADER_SIZE], 0x77);
        drop(guard);
        assert!(engine.recovered_active_xids().is_empty());
        assert_eq!(engine.next_oid(), Oid::FIRST_USER);

        // Forward crash protection (§11.4): recovery is read-only — the v1
        // record was NOT rewritten as v2.
        let ends = checkpoint_end_records(tmp.path(), config.wal_segment_size, Lsn::FIRST);
        assert_eq!(ends.len(), 1, "exactly one CheckpointEnd so far");
        let v1 = &ends[0];
        assert_eq!(v1.flags, 0, "recovery must not rewrite v1 CheckpointEnd");
        let decoded = CheckpointEndRecord::decode(&v1.payload, v1.flags).unwrap();
        assert_eq!(decoded.checkpoint_lsn, v1_checkpoint_lsn);
        assert_eq!(decoded.next_oid, Oid::FIRST_USER.0);
        assert!(decoded.att_file.is_empty());
        assert!(decoded.dpt_file.is_empty());

        // -- Phase 3: the next M2 checkpoint upgrades the format -----------
        let begin2 = engine.trigger_checkpoint().unwrap();
        assert!(begin2 > v1_checkpoint_lsn);

        let ends = checkpoint_end_records(tmp.path(), config.wal_segment_size, Lsn::FIRST);
        assert_eq!(ends.len(), 2);
        assert_eq!(ends[0].flags, 0, "the old record stays v1");

        let v2 = &ends[1];
        assert_eq!(v2.flags, CHECKPOINT_END_V2_FLAGS);
        let decoded = CheckpointEndRecord::decode(&v2.payload, v2.flags).unwrap();
        assert_eq!(decoded.checkpoint_lsn, begin2);
        assert_eq!(decoded.next_oid, Oid::FIRST_USER.0);
        assert!(!decoded.att_file.is_empty());
        assert!(!decoded.dpt_file.is_empty());
        assert!(
            tmp.path().join(&decoded.att_file).exists(),
            "ATT snapshot file must exist: {}",
            decoded.att_file
        );
        assert!(
            tmp.path().join(&decoded.dpt_file).exists(),
            "DPT snapshot file must exist: {}",
            decoded.dpt_file
        );
    }

    // -- Phase 4: reopen after the upgrade — v2 snapshot path -------------
    {
        let engine = StorageEngine::open(tmp.path(), &config).unwrap();
        let guard = engine.buffer_pool().pin(page_id).unwrap();
        assert_eq!(guard.page()[PAGE_HEADER_SIZE], 0x77);
    }
}
