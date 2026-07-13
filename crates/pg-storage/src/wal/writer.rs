//! WAL writer with group-commit.
//!
//! The writer owns the LSN clock and the WAL segment manager. It serializes
//! records, writes them to the current WAL segment, and fsyncs them in batches
//! using a dedicated background thread.

use std::path::Path;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::config::StorageConfig;
use crate::error::{Result, StorageError};
use crate::lsn_clock::LsnClock;
use crate::types::{Lsn, LSN_ALIGNMENT};
use crate::wal::reader::WalReader;
use crate::wal::record::WalRecord;
use crate::wal::segment::WalSegmentManager;

/// A thread-safe WAL writer.
#[derive(Debug)]
pub struct WalWriter {
    inner: Arc<Mutex<WriterState>>,
    cond: Arc<Condvar>,
    config: StorageConfig,
    handle: Option<JoinHandle<()>>,
}

#[derive(Debug)]
struct WriterState {
    lsn_clock: LsnClock,
    segment_manager: WalSegmentManager,
    synced_lsn: Lsn,
    pending: usize,
    last_flush: Instant,
    shutdown: bool,
    last_error: Option<String>,
}

impl WalWriter {
    /// Open or create the WAL writer in `{data_dir}/wal`, starting at
    /// [`Lsn::FIRST`].
    ///
    /// If WAL segment files already exist (for example after a crash or a
    /// previous run), the writer scans the durable log and resumes appending
    /// from the byte position immediately after the last complete record. This
    /// prevents recovery code from accidentally overwriting existing WAL.
    ///
    /// # Preconditions
    ///
    /// This method assumes the complete WAL is still present in `{data_dir}/wal`
    /// (i.e. no segments have been recycled by a checkpoint). If older segments
    /// are missing, use [`Self::open_at`] with the checkpoint redo LSN instead.
    pub fn open(data_dir: impl AsRef<Path>, config: &StorageConfig) -> Result<Self> {
        let wal_dir = data_dir.as_ref().join("wal");
        let mut segment_manager = WalSegmentManager::open(&wal_dir, config.wal_segment_size)?;

        let has_existing_records = segment_manager.current_segment_id() > 0
            || segment_manager
                .current_file()
                .metadata()
                .map_err(StorageError::Io)?
                .len()
                > 0;

        let start_lsn = if has_existing_records {
            Self::discover_resume_lsn(&wal_dir, config.wal_segment_size)?
        } else {
            Lsn::FIRST
        };

        Self::open_with_segment_manager(segment_manager, config, start_lsn)
    }

    /// Open or create the WAL writer in `{data_dir}/wal`, starting at
    /// `start_lsn`.
    ///
    /// The caller (Stage I recovery) is responsible for ensuring that the WAL
    /// segment files on disk are consistent with `start_lsn`: the latest
    /// existing segment must be the one that contains `start_lsn`. If a later
    /// segment exists, this method returns an error so that recovery can trim
    /// or recycle it first.
    ///
    /// For normal restart, prefer [`Self::open`] which resumes from the end of
    /// the existing WAL automatically.
    pub fn open_at(
        data_dir: impl AsRef<Path>,
        config: &StorageConfig,
        start_lsn: Lsn,
    ) -> Result<Self> {
        if !start_lsn.is_valid() || start_lsn.0 % LSN_ALIGNMENT != 0 {
            return Err(StorageError::InvalidConfig(format!(
                "invalid WAL start LSN {start_lsn}"
            )));
        }

        let wal_dir = data_dir.as_ref().join("wal");
        let segment_manager = WalSegmentManager::open(wal_dir, config.wal_segment_size)?;

        let expected_segment = start_lsn.segment_id(config.wal_segment_size);
        if expected_segment != segment_manager.current_segment_id() {
            return Err(StorageError::InvalidConfig(format!(
                "start_lsn {start_lsn} is in segment {expected_segment} but latest WAL segment is {}; recovery must trim or recycle segments first",
                segment_manager.current_segment_id()
            )));
        }

        Self::open_with_segment_manager(segment_manager, config, start_lsn)
    }

    fn open_with_segment_manager(
        segment_manager: WalSegmentManager,
        config: &StorageConfig,
        start_lsn: Lsn,
    ) -> Result<Self> {
        let lsn_clock = LsnClock::new(start_lsn);

        let inner = Arc::new(Mutex::new(WriterState {
            lsn_clock,
            segment_manager,
            synced_lsn: Lsn::INVALID,
            pending: 0,
            last_flush: Instant::now(),
            shutdown: false,
            last_error: None,
        }));
        let cond = Arc::new(Condvar::new());

        let handle = {
            let inner = Arc::clone(&inner);
            let cond = Arc::clone(&cond);
            let config = config.clone();
            thread::spawn(move || Self::worker(inner, cond, config))
        };

        Ok(Self {
            inner,
            cond,
            config: config.clone(),
            handle: Some(handle),
        })
    }

    /// Scan the existing WAL and return the LSN immediately after the last
    /// complete record. Torn or partial records at the tail are discarded: the
    /// returned LSN points to the start of the torn record, allowing the next
    /// append to overwrite it.
    fn discover_resume_lsn(wal_dir: &Path, segment_size: u64) -> Result<Lsn> {
        let mut reader = WalReader::open_at(wal_dir, segment_size, Lsn::FIRST)?;
        while reader.next_record()?.is_some() {}
        Ok(reader.current_lsn())
    }

    /// Append a record to the WAL.
    ///
    /// The record is assigned an LSN, written to the current segment file, and
    /// the method returns only after the record has been fsynced to disk.
    /// Concurrent callers are batched into a single fsync by the background
    /// worker.
    pub fn append(&self, mut record: WalRecord) -> Result<Lsn> {
        let lsn = {
            let mut state = self.inner.lock();
            Self::check_error(&state)?;

            let record_size = record.record_size() as u64;
            let lsn = state.lsn_clock.next(record_size);
            record.lsn = lsn;

            let buf = record.encode()?;
            write_record_to_segment(&mut state.segment_manager, &buf, lsn)?;

            state.pending += 1;
            let timeout = Duration::from_millis(self.config.wal_group_commit_timeout_ms);
            let should_wake = state.pending >= self.config.wal_group_commit_batch_size
                || state.last_flush.elapsed() >= timeout;
            if should_wake {
                self.cond.notify_one();
            }
            lsn
        };

        self.flush_to(lsn)?;
        Ok(lsn)
    }

    /// Flush all records that have been appended up to this point.
    ///
    /// If no records are pending this returns immediately.
    pub fn flush(&self) -> Result<()> {
        let target = {
            let state = self.inner.lock();
            Self::check_error(&state)?;
            if state.pending == 0 {
                return Ok(());
            }
            // synced_lsn is advanced to lsn_clock.current() (the byte
            // immediately following the last written record) on each flush,
            // so waiting for current itself covers every pending byte.
            state.lsn_clock.current()
        };
        self.flush_to(target)
    }

    /// Block until all records with LSN `<= lsn` have been fsynced.
    pub fn flush_to(&self, lsn: Lsn) -> Result<()> {
        let mut state = self.inner.lock();
        Self::check_error(&state)?;

        if lsn > state.lsn_clock.current() {
            return Err(StorageError::LsnNotAvailable(lsn));
        }
        if state.synced_lsn >= lsn {
            return Ok(());
        }

        // Wake the worker so it flushes immediately rather than waiting for the
        // timeout.
        self.cond.notify_one();

        while state.synced_lsn < lsn {
            Self::check_error(&state)?;
            if state.shutdown {
                return Err(StorageError::WalWriteFailed(
                    "wal writer shut down".to_string(),
                ));
            }
            self.cond.wait(&mut state);
        }
        Ok(())
    }

    /// Return the latest LSN that has been fsynced to disk.
    pub fn synced_lsn(&self) -> Lsn {
        self.inner.lock().synced_lsn
    }

    fn check_error(state: &WriterState) -> Result<()> {
        if let Some(ref msg) = state.last_error {
            return Err(StorageError::WalWriteFailed(msg.clone()));
        }
        Ok(())
    }

    fn worker(inner: Arc<Mutex<WriterState>>, cond: Arc<Condvar>, config: StorageConfig) {
        let timeout = Duration::from_millis(config.wal_group_commit_timeout_ms);

        loop {
            let mut state = inner.lock();

            if state.shutdown && state.pending == 0 {
                break;
            }

            let should_flush = state.pending > 0
                && (state.pending >= config.wal_group_commit_batch_size
                    || state.last_flush.elapsed() >= timeout);

            if !should_flush && !state.shutdown {
                cond.wait_for(&mut state, timeout);
                continue;
            }

            if state.pending == 0 {
                // Timeout with nothing to do, or woken spuriously.
                if state.shutdown {
                    break;
                }
                continue;
            }

            // Perform the fsync. On failure record the error, mark the writer
            // as shut down, and wake all waiters. The worker exits so that it
            // does not retry fsync on a potentially corrupted state; subsequent
            // appends will fail with the recorded error.
            //
            // TODO(M2+): there is no recovery path today. Future work could
            // reopen/rotate the segment or surface the error to the caller for
            // a higher-level decision.
            if let Err(e) = state.segment_manager.current_file().sync_all() {
                state.last_error = Some(format!("wal fsync failed: {e}"));
                state.shutdown = true;
                cond.notify_all();
                break;
            }

            // Fsync succeeded: clear any transient error and advance synced_lsn.
            state.last_error = None;
            state.synced_lsn = state.lsn_clock.current();
            state.pending = 0;
            state.last_flush = Instant::now();
            cond.notify_all();
        }
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        {
            let mut state = self.inner.lock();
            state.shutdown = true;
        }
        self.cond.notify_one();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Write a serialized record to the WAL segment(s), handling cross-segment
/// records by splitting the write at the segment boundary.
fn write_record_to_segment(
    segment_manager: &mut WalSegmentManager,
    buf: &[u8],
    lsn: Lsn,
) -> Result<()> {
    use std::io::{Seek, SeekFrom, Write};

    let segment_size = segment_manager.segment_size();
    if buf.len() as u64 > segment_size {
        return Err(StorageError::WalWriteFailed(format!(
            "record size {} exceeds segment size {}",
            buf.len(),
            segment_size
        )));
    }

    let mut written = 0;
    while written < buf.len() {
        let pos = Lsn(lsn.0 + written as u64);
        let offset = pos.segment_offset(segment_size);
        let remaining_in_segment = (segment_size - offset) as usize;
        let chunk = std::cmp::min(remaining_in_segment, buf.len() - written);

        let file = segment_manager.ensure_for_write(pos)?;
        file.seek(SeekFrom::Start(offset))
            .map_err(StorageError::Io)?;
        file.write_all(&buf[written..written + chunk])
            .map_err(StorageError::Io)?;

        written += chunk;

        // If this chunk filled the segment and there is more data, fsync the
        // current segment before switching to the next one.
        if written < buf.len() && offset + chunk as u64 == segment_size {
            file.sync_all().map_err(StorageError::Io)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::io::Read;
    use std::sync::Arc;
    use std::thread;

    use crate::types::PageId;
    use crate::wal::record::WalRecordType;
    use tempfile::TempDir;

    fn writer_config(tmp: &TempDir) -> StorageConfig {
        let mut cfg = StorageConfig::new(tmp.path());
        cfg.wal_group_commit_timeout_ms = 5;
        cfg.wal_group_commit_batch_size = 4;
        cfg.wal_segment_size = 1024;
        cfg
    }

    #[test]
    fn append_and_read_single_record() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        let lsn = writer
            .append(WalRecord::page_alloc(PageId(42)).unwrap())
            .unwrap();
        assert!(lsn.is_valid());
        assert!(writer.synced_lsn() >= lsn);

        // Read the record back from the WAL segment.
        let path = tmp.path().join("wal").join("wal-00000001.log");
        let mut file = fs::File::open(&path).unwrap();
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).unwrap();

        let (decoded, _) =
            WalRecord::decode(&buf[lsn.segment_offset(cfg.wal_segment_size) as usize..]).unwrap();
        assert_eq!(decoded.record_type, WalRecordType::PageAlloc);
        assert_eq!(decoded.lsn, lsn);
    }

    #[test]
    fn append_many_records_preserves_order() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        let count = 100;
        let mut lsns = Vec::new();
        for i in 0..count {
            let lsn = writer
                .append(WalRecord::page_alloc(PageId(i + 1)).unwrap())
                .unwrap();
            lsns.push(lsn);
        }

        for window in lsns.windows(2) {
            assert!(window[1] > window[0]);
        }
        assert!(writer.synced_lsn() >= lsns[lsns.len() - 1]);
    }

    #[test]
    fn concurrent_appends_are_monotonic() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());

        let mut handles = Vec::new();
        for i in 0..8 {
            let w = Arc::clone(&writer);
            handles.push(thread::spawn(move || {
                let mut lsns = Vec::new();
                for j in 0..50 {
                    let lsn = w
                        .append(WalRecord::page_alloc(PageId(i * 50 + j + 1)).unwrap())
                        .unwrap();
                    lsns.push(lsn);
                }
                lsns
            }));
        }

        let mut all = Vec::new();
        for h in handles {
            all.extend(h.join().unwrap());
        }

        all.sort_unstable();
        for window in all.windows(2) {
            assert!(window[1] > window[0]);
        }
    }

    #[test]
    fn cross_segment_write() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = writer_config(&tmp);
        cfg.wal_segment_size = 256;
        cfg.wal_group_commit_batch_size = 1;
        cfg.wal_group_commit_timeout_ms = 1; // flush quickly, but avoid a 0 timeout busy-loop
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        // Build a record that is larger than the remaining space at the end of
        // a segment, so it must be split across two segment files.
        let fpi = WalRecord::full_page_image(PageId(99), vec![0xCD; 32]).unwrap();
        let fpi_size = fpi.record_size() as u64;
        assert!(fpi_size <= cfg.wal_segment_size);

        let small = WalRecord::page_alloc(PageId(1)).unwrap();
        let small_size = small.record_size() as u64;

        // Append small records until the next record would start close enough to
        // the segment boundary that the FPI crosses over.
        let mut last_lsn = writer.append(small.clone()).unwrap();
        loop {
            let offset = last_lsn.segment_offset(cfg.wal_segment_size);
            if offset + small_size + fpi_size > cfg.wal_segment_size
                && offset + fpi_size > cfg.wal_segment_size
            {
                break;
            }
            last_lsn = writer.append(small.clone()).unwrap();
        }

        let fpi_lsn = writer.append(fpi).unwrap();
        // The FPI starts in the same segment as the last small record but its
        // tail crosses the segment boundary, so a second segment file must exist.
        assert_eq!(
            fpi_lsn.segment_id(cfg.wal_segment_size),
            last_lsn.segment_id(cfg.wal_segment_size)
        );
        let fpi_end_segment = Lsn(fpi_lsn.0 + fpi_size - 1).segment_id(cfg.wal_segment_size);
        assert!(fpi_end_segment > fpi_lsn.segment_id(cfg.wal_segment_size));
        assert!(tmp.path().join("wal").join("wal-00000001.log").exists());
        assert!(tmp.path().join("wal").join("wal-00000002.log").exists());
    }

    #[test]
    fn flush_on_empty_writer_returns_immediately() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();
        writer.flush().unwrap();
    }

    #[test]
    fn append_wakes_worker_for_sync() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = writer_config(&tmp);
        cfg.wal_group_commit_timeout_ms = 1000; // long timeout
        cfg.wal_group_commit_batch_size = 100;
        let writer = Arc::new(WalWriter::open(tmp.path(), &cfg).unwrap());

        let w = Arc::clone(&writer);
        let handle =
            thread::spawn(move || w.append(WalRecord::page_alloc(PageId(1)).unwrap()).unwrap());

        // append internally calls flush_to, which should wake the worker and
        // return once the background fsync completes.
        let lsn = handle.join().unwrap();
        assert!(writer.synced_lsn() >= lsn);
    }

    #[test]
    fn flush_to_is_idempotent_after_sync() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        let lsn = writer
            .append(WalRecord::page_alloc(PageId(1)).unwrap())
            .unwrap();
        // Calling flush_to directly on an already-synced LSN must return
        // immediately without error.
        writer.flush_to(lsn).unwrap();
        assert!(writer.synced_lsn() >= lsn);
    }

    #[test]
    fn open_at_rejects_invalid_and_unaligned_lsn() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        assert!(WalWriter::open_at(tmp.path(), &cfg, Lsn::INVALID).is_err());
        assert!(WalWriter::open_at(tmp.path(), &cfg, Lsn(12)).is_err());
    }

    #[test]
    fn open_at_rejects_lsn_in_older_segment() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = writer_config(&tmp);
        cfg.wal_segment_size = 64;

        // Pre-create segment 1 so the segment manager discovers it as latest.
        let wal_dir = tmp.path().join("wal");
        fs::create_dir(&wal_dir).unwrap();
        fs::File::create(wal_dir.join("wal-00000002.log")).unwrap();

        // Lsn::FIRST lives in segment 0, but the latest segment is 1.
        assert!(WalWriter::open_at(tmp.path(), &cfg, Lsn::FIRST).is_err());
    }

    #[test]
    fn open_at_accepts_lsn_in_latest_segment() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = writer_config(&tmp);
        cfg.wal_segment_size = 64;
        cfg.wal_group_commit_batch_size = 1;
        cfg.wal_group_commit_timeout_ms = 1;

        // Pre-create segment 1 so the segment manager discovers it as latest.
        let wal_dir = tmp.path().join("wal");
        fs::create_dir(&wal_dir).unwrap();
        fs::File::create(wal_dir.join("wal-00000002.log")).unwrap();

        let writer = WalWriter::open_at(tmp.path(), &cfg, Lsn(64)).unwrap();
        let lsn = writer
            .append(WalRecord::page_alloc(PageId(1)).unwrap())
            .unwrap();
        assert!(lsn >= Lsn(64));
    }
}
