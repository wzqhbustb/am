//! One-time migration of the M2a `clog-snapshot.bin` bridge (Stage L).
//!
//! The M2a engine persisted pre-checkpoint commit/abort states in
//! `{data_dir}/clog-snapshot.bin` (its WAL prefix had already been recycled
//! by checkpoints, so those states existed nowhere else). Stage L replaced
//! the whole bridge with the disk-backed [`ClogBuffer`] — but a data
//! directory written by the M2a engine and opened by the M2b engine would
//! otherwise lose exactly those states: the new disk CLOG starts empty and
//! replay only covers the post-checkpoint WAL suffix (silent data loss,
//! Stage L review F3, and a violation of the on-disk compatibility promise
//! made at the `phase1-m2a` tag).
//!
//! [`migrate_legacy_clog_snapshot`] therefore loads any leftover snapshot
//! into the disk CLOG at open and renames the file to
//! `clog-snapshot.bin.migrated` (kept for audit; re-runs are naturally
//! idempotent because only the original name is migrated).
//!
//! The on-disk format below is frozen M2a history (write side deleted with
//! the bridge); this reader is the only consumer and must never change.

use std::path::{Path, PathBuf};

use pg_storage::clog::{ClogAccessor, TxnState};
use pg_storage::error::StorageError;
use pg_storage::types::TxnId;
use pg_txn::ClogBuffer;

use crate::error::{EngineError, Result};

/// Snapshot file magic (`"PGRUSTCL"`), M2a bridge format.
const MAGIC: u64 = 0x5047_5255_5354_434C;
/// Snapshot format version (only 1 ever shipped).
const VERSION: u32 = 1;
/// M2a snapshot file name.
const LEGACY_NAME: &str = "clog-snapshot.bin";
/// Name the file is renamed to after a successful migration.
const MIGRATED_NAME: &str = "clog-snapshot.bin.migrated";

fn migrated_path(data_dir: &Path) -> PathBuf {
    data_dir.join(MIGRATED_NAME)
}

/// Load the M2a `clog-snapshot.bin` (if present) into `clog`, then rename it
/// out of the way.
///
/// - Missing file: no-op (normal for M2b-native directories).
/// - Corrupt file: hard error. The M2a loader also hard-failed on
///   corruption; silently dropping M2a commit states would be worse.
pub fn migrate_legacy_clog_snapshot(data_dir: &Path, clog: &ClogBuffer) -> Result<()> {
    let path = data_dir.join(LEGACY_NAME);
    let buf = match std::fs::read(&path) {
        Ok(buf) => buf,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(EngineError::Storage(StorageError::Io(e))),
    };

    let entries = parse(&buf).map_err(|msg| {
        EngineError::Corrupted(format!("legacy clog snapshot {}: {msg}", path.display()))
    })?;
    let count = entries.len();
    for (xid, state) in entries {
        clog.set_state(xid, state);
    }

    // Make the migrated states durable BEFORE retiring the legacy file:
    // `set_state` only dirties in-memory frames, and the TxnCommit WAL
    // records backing these XIDs were already recycled by M2a checkpoints —
    // so a crash after the rename but before the first M2b checkpoint would
    // lose them for good (Stage L review: migration crash window). If the
    // flush fails, `.bin` stays in place and the next open retries.
    clog.flush_dirty()?;

    std::fs::rename(&path, migrated_path(data_dir))
        .map_err(|e| EngineError::Storage(StorageError::Io(e)))?;
    tracing::info!(count, "migrated M2a clog-snapshot.bin into the disk CLOG");
    Ok(())
}

/// Parse the M2a format: `MAGIC(8) | VERSION(4) | count(8) | (xid(8),
/// state(1))* | fnv1a(8)`. States: 1 = Committed, 2 = Aborted.
fn parse(buf: &[u8]) -> std::result::Result<Vec<(TxnId, TxnState)>, String> {
    if buf.len() < 8 + 4 + 8 + 8 {
        return Err(format!("file too short ({} bytes)", buf.len()));
    }
    let (body, checksum_bytes) = buf.split_at(buf.len() - 8);
    let stored = u64::from_le_bytes(checksum_bytes.try_into().expect("8 bytes"));
    if fnv1a(body) != stored {
        return Err("checksum mismatch".to_string());
    }
    let magic = u64::from_le_bytes(body[0..8].try_into().expect("8 bytes"));
    if magic != MAGIC {
        return Err("bad magic".to_string());
    }
    let version = u32::from_le_bytes(body[8..12].try_into().expect("4 bytes"));
    if version != VERSION {
        return Err(format!("unsupported version {version}"));
    }
    let count = u64::from_le_bytes(body[12..20].try_into().expect("8 bytes")) as usize;
    let entries_bytes = &body[20..];
    if entries_bytes.len() != count * 9 {
        return Err(format!(
            "entry count {count} does not match payload size {}",
            entries_bytes.len()
        ));
    }
    let mut entries = Vec::with_capacity(count);
    for chunk in entries_bytes.chunks_exact(9) {
        let xid = TxnId(u64::from_le_bytes(chunk[0..8].try_into().expect("8 bytes")));
        let state = match chunk[8] {
            1 => TxnState::Committed,
            2 => TxnState::Aborted,
            other => return Err(format!("unknown state byte {other} for xid {xid}")),
        };
        entries.push((xid, state));
    }
    Ok(entries)
}

/// FNV-1a, matching the M2a writer exactly.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a legacy snapshot buffer exactly as the M2a writer did.
    fn legacy_bytes(entries: &[(u64, u8)]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC.to_le_bytes());
        buf.extend_from_slice(&VERSION.to_le_bytes());
        buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());
        for (xid, state) in entries {
            buf.extend_from_slice(&xid.to_le_bytes());
            buf.push(*state);
        }
        let checksum = fnv1a(&buf);
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf
    }

    #[test]
    fn parse_round_trips_legacy_format() {
        let buf = legacy_bytes(&[(3, 1), (4, 2)]);
        let entries = parse(&buf).unwrap();
        assert_eq!(
            entries,
            vec![
                (TxnId(3), TxnState::Committed),
                (TxnId(4), TxnState::Aborted)
            ]
        );
    }

    #[test]
    fn parse_rejects_checksum_mismatch() {
        let mut buf = legacy_bytes(&[(3, 1)]);
        let n = buf.len();
        buf[n - 1] ^= 0xFF;
        assert!(parse(&buf).is_err());
    }
}
