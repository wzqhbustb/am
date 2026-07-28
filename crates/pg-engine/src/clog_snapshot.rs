//! CLOG durability across checkpoints (M2a bridge; coding-plan Stage K).
//!
//! The M2a commit log is the in-memory [`InMemoryClogAccessor`]: the
//! authoritative source is the WAL, and recovery rebuilds commit/abort state
//! by replaying `TxnCommit` / `TxnAbort` records (Stage J). That design has a
//! gap once a **checkpoint** enters the picture: recovery replays only from
//! the checkpoint redo point, so every commit record *before* it is never
//! replayed — and a CLOG miss reads as `InProgress`, i.e. **invisible**.
//! "commit → checkpoint → restart" would lose the committed state of every
//! row written before the checkpoint. (M2b replaces the whole mechanism with
//! the disk-backed CLOG SLRU; this module is the M2a-only bridge.)
//!
//! The bridge, entirely inside `pg-engine`:
//!
//! - [`TrackingClog`] wraps the `InMemoryClogAccessor` and records every
//!   terminal state ever set through it — by WAL replay, by `commit_txn` /
//!   `abort_txn`, and by the snapshot load below — so at any moment its
//!   `terminal_entries()` is the full committed/aborted history the running
//!   engine has observed.
//! - [`Engine::checkpoint`](crate::Engine::checkpoint) first dumps those
//!   entries to `{data_dir}/clog-snapshot.bin` (atomic tmp + rename), **then**
//!   triggers the storage checkpoint that advances the redo point and may
//!   recycle WAL segments. The dump-before-truncate order is load-bearing: a
//!   crash before the truncate leaves the full WAL to replay (the snapshot is
//!   an idempotent subset); a crash after it leaves the full snapshot.
//! - [`Engine::open`](crate::Engine::open) loads the snapshot into the CLOG
//!   right after recovery replay. Replay covered everything after the last
//!   checkpoint, the snapshot everything before it; `set_state` is idempotent
//!   and both describe the same history, so the union is exact.
//!
//! Because the dump goes through `set_state`, the snapshot's entries are
//! themselves tracked: the next checkpoint's dump is again the full history,
//! not just the current session's transactions.
//!
//! Limitations (M2a, removed in M2b together with the file):
//!
//! - Background checkpointing would bypass the dump, so the M2a engine never
//!   starts it; callers must go through `Engine::checkpoint`.
//! - The file grows monotonically (never-GC, v2.3-2), like the CLOG itself.
//! - Committing through a bare `TxnManager` obtained from
//!   [`Engine::txn_manager`](crate::Engine::txn_manager) still passes through
//!   `TrackingClog::set_state`, so even that back door is tracked.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use pg_storage::clog::{ClogAccessor, TxnState};
use pg_storage::types::TxnId;
use pg_txn::InMemoryClogAccessor;

use crate::error::{EngineError, Result};

/// Snapshot file magic (`"PGRUSTCL"`).
const MAGIC: u64 = 0x5047_5255_5354_434C;
/// Snapshot format version.
const VERSION: u32 = 1;
/// File name of the CLOG snapshot inside the data directory.
const FILE_NAME: &str = "clog-snapshot.bin";
/// Scratch name the snapshot is written to before the atomic rename.
const TMP_NAME: &str = "clog-snapshot.tmp";

/// An [`InMemoryClogAccessor`] that additionally remembers every terminal
/// state set through it (see the module docs).
#[derive(Debug)]
pub struct TrackingClog {
    inner: InMemoryClogAccessor,
    /// Full committed/aborted history observed by this engine, in XID order.
    terminal: Mutex<BTreeMap<TxnId, TxnState>>,
}

impl TrackingClog {
    /// Create an empty tracker (GC stays disabled, as in M2a everywhere).
    pub fn new() -> Self {
        Self {
            inner: InMemoryClogAccessor::new(),
            terminal: Mutex::new(BTreeMap::new()),
        }
    }

    /// The tracked terminal states in ascending XID order — the payload of
    /// the checkpoint-time snapshot dump.
    pub fn terminal_entries(&self) -> Vec<(TxnId, TxnState)> {
        self.terminal
            .lock()
            .expect("tracking clog poisoned")
            .iter()
            .map(|(&xid, &state)| (xid, state))
            .collect()
    }
}

impl Default for TrackingClog {
    fn default() -> Self {
        Self::new()
    }
}

impl ClogAccessor for TrackingClog {
    fn get_state(&self, xid: TxnId) -> TxnState {
        self.inner.get_state(xid)
    }

    fn set_state(&self, xid: TxnId, state: TxnState) {
        self.inner.set_state(xid, state);
        if matches!(state, TxnState::Committed | TxnState::Aborted) {
            self.terminal
                .lock()
                .expect("tracking clog poisoned")
                .insert(xid, state);
        }
    }
}

/// The snapshot file's path inside `data_dir`.
fn snapshot_path(data_dir: &Path) -> PathBuf {
    data_dir.join(FILE_NAME)
}

/// Persist `entries` as the CLOG snapshot of `data_dir`, atomically.
///
/// Write-tmp-fsync-rename, then fsync the directory so the rename itself is
/// durable: after `Ok(())` the caller may let a checkpoint truncate the WAL
/// prefix this snapshot replaces.
pub fn write_clog_snapshot(data_dir: &Path, entries: &[(TxnId, TxnState)]) -> Result<()> {
    let mut buf = Vec::with_capacity(8 + 4 + 8 + entries.len() * 9 + 8);
    buf.extend_from_slice(&MAGIC.to_le_bytes());
    buf.extend_from_slice(&VERSION.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for (xid, state) in entries {
        buf.extend_from_slice(&xid.0.to_le_bytes());
        buf.push(match state {
            TxnState::Committed => 1,
            TxnState::Aborted => 2,
            // TrackingClog only records terminal states; anything else here
            // is a caller bug and must not reach disk.
            other => {
                return Err(EngineError::Corrupted(format!(
                    "clog snapshot asked to persist non-terminal state {other:?}"
                )))
            }
        });
    }
    let checksum = fnv1a(&buf);
    buf.extend_from_slice(&checksum.to_le_bytes());

    let tmp_path = data_dir.join(TMP_NAME);
    std::fs::write(&tmp_path, &buf).map_err(EngineError::storage_io)?;
    std::fs::File::open(&tmp_path)
        .and_then(|f| f.sync_all())
        .map_err(EngineError::storage_io)?;
    std::fs::rename(&tmp_path, snapshot_path(data_dir)).map_err(EngineError::storage_io)?;
    // Make the rename itself durable.
    std::fs::File::open(data_dir)
        .and_then(|d| d.sync_all())
        .map_err(EngineError::storage_io)?;
    Ok(())
}

/// Load the CLOG snapshot of `data_dir`, if any.
///
/// A missing file is normal (no checkpoint ever ran) and yields an empty
/// vector — recovery replay alone rebuilds the CLOG in that case. A
/// malformed file is [`EngineError::Corrupted`]: the atomic rename means a
/// torn snapshot should be impossible, so damage here is real corruption and
/// must not be silently degraded into "old rows read invisible".
pub fn load_clog_snapshot(data_dir: &Path) -> Result<Vec<(TxnId, TxnState)>> {
    let path = snapshot_path(data_dir);
    let buf = match std::fs::read(&path) {
        Ok(buf) => buf,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(EngineError::storage_io(e)),
    };
    let corrupt = |msg: String| EngineError::Corrupted(format!("{}: {msg}", path.display()));

    if buf.len() < 8 + 4 + 8 + 8 {
        return Err(corrupt(format!("file too short ({} bytes)", buf.len())));
    }
    let (body, checksum_bytes) = buf.split_at(buf.len() - 8);
    let stored_checksum = u64::from_le_bytes(checksum_bytes.try_into().expect("8 bytes"));
    if fnv1a(body) != stored_checksum {
        return Err(corrupt("checksum mismatch".to_string()));
    }
    let magic = u64::from_le_bytes(body[0..8].try_into().expect("8 bytes"));
    if magic != MAGIC {
        return Err(corrupt("bad magic".to_string()));
    }
    let version = u32::from_le_bytes(body[8..12].try_into().expect("4 bytes"));
    if version != VERSION {
        return Err(corrupt(format!("unsupported version {version}")));
    }
    let count = u64::from_le_bytes(body[12..20].try_into().expect("8 bytes")) as usize;
    let entries_bytes = &body[20..];
    if entries_bytes.len() != count * 9 {
        return Err(corrupt(format!(
            "entry count {count} does not match payload size {}",
            entries_bytes.len()
        )));
    }

    let mut entries = Vec::with_capacity(count);
    for chunk in entries_bytes.chunks_exact(9) {
        let xid = TxnId(u64::from_le_bytes(chunk[0..8].try_into().expect("8 bytes")));
        let state = match chunk[8] {
            1 => TxnState::Committed,
            2 => TxnState::Aborted,
            other => {
                return Err(corrupt(format!("unknown state byte {other} for xid {xid}")));
            }
        };
        entries.push((xid, state));
    }
    Ok(entries)
}

/// FNV-1a over `bytes` — a cheap integrity check for the snapshot (the data
/// directory has no stronger checksum infrastructure in M2a).
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
