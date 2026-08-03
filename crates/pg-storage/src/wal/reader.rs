//! WAL reader for sequential record access.
//!
//! The reader treats the WAL as a single logical byte stream split across fixed-
//! size segment files. It can start at any aligned LSN and reads records
//! sequentially, automatically opening the next segment file when required.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::error::{Result, StorageError};
use crate::types::{align_up, Lsn, LSN_ALIGNMENT};
use crate::wal::record::{WalRecord, WAL_RECORD_HEADER_SIZE};
use crate::wal::segment::wal_filename;

/// A sequential WAL reader.
///
/// `WalReader` is not thread-safe; the caller is expected to serialize access
/// or create one reader per thread.
#[derive(Debug)]
pub struct WalReader {
    wal_dir: PathBuf,
    segment_size: u64,
    current_segment_id: u64,
    current_file: File,
    current_lsn: Lsn,
}

impl WalReader {
    /// Open a reader starting at [`Lsn::FIRST`] in `wal_dir`.
    pub fn open(wal_dir: impl AsRef<Path>, segment_size: u64) -> Result<Self> {
        Self::open_at(wal_dir, segment_size, Lsn::FIRST)
    }

    /// Open a reader starting at `start_lsn`.
    ///
    /// `start_lsn` must be valid, aligned, and point to the first byte of a
    /// record (typically a checkpoint redo point). If `start_lsn` falls in the
    /// middle of a record, decoding will fail. The caller is responsible for
    /// ensuring that the corresponding segment file exists.
    pub fn open_at(wal_dir: impl AsRef<Path>, segment_size: u64, start_lsn: Lsn) -> Result<Self> {
        if !start_lsn.is_valid() || start_lsn.0 % LSN_ALIGNMENT != 0 {
            return Err(StorageError::InvalidConfig(format!(
                "invalid WAL start LSN {start_lsn}"
            )));
        }
        if segment_size == 0 || segment_size % LSN_ALIGNMENT != 0 {
            return Err(StorageError::InvalidConfig(format!(
                "invalid WAL segment size {segment_size}"
            )));
        }

        let wal_dir = wal_dir.as_ref().to_path_buf();
        let segment_id = start_lsn.segment_id(segment_size);
        let file = Self::open_segment_file(&wal_dir, segment_id)?;

        Ok(Self {
            wal_dir,
            segment_size,
            current_segment_id: segment_id,
            current_file: file,
            current_lsn: start_lsn,
        })
    }

    /// Return the LSN at which the next record read will begin.
    pub fn current_lsn(&self) -> Lsn {
        self.current_lsn
    }

    /// Read the next record from the WAL.
    ///
    /// Returns `Ok(None)` when the end of the durable WAL is reached. An all-
    /// zero header (uninitialized padding) is treated as a clean end-of-WAL
    /// marker. Any other decode or CRC failure is returned as an error.
    pub fn next_record(&mut self) -> Result<Option<WalRecord>> {
        let start_lsn = self.current_lsn;
        let mut header = [0u8; WAL_RECORD_HEADER_SIZE];
        if !self.try_read_exact(&mut header)? {
            return Ok(None);
        }

        // An all-zero header means we have reached the uninitialized tail of
        // the WAL — or a zero-filled hole left by a non-atomic reserve+append
        // (Stage N review, P0-3). Before treating it as end-of-WAL, probe
        // forward one header: if that candidate is also all zeros, we're at
        // the preallocated tail. If it is non-zero, we've hit a hole — a
        // crash filled a reserved slot that was never written, and the
        // records after it are silently lost if we return Ok(None) here.
        // Hard-fail instead.
        if header.iter().all(|&b| b == 0) {
            let probe_pos = start_lsn.0 + WAL_RECORD_HEADER_SIZE as u64;
            let mut probe = [0u8; WAL_RECORD_HEADER_SIZE];
            if self.try_read_exact_at(probe_pos, &mut probe)?
                && probe.iter().any(|&b| b != 0)
            {
                return Err(StorageError::MetadataCorrupted(format!(
                    "WAL hole detected: zero-filled header at {:?} followed by non-zero data at {:?}; \
                     an unflushed reserved slot truncated the log",
                    start_lsn, Lsn(probe_pos)
                )));
            }
            self.current_lsn = start_lsn;
            return Ok(None);
        }

        let payload_len = u16::from_le_bytes(header[26..28].try_into().unwrap()) as usize;
        let total = align_up(WAL_RECORD_HEADER_SIZE + payload_len, 8);

        let mut buf = Vec::with_capacity(total);
        buf.extend_from_slice(&header);

        if total > WAL_RECORD_HEADER_SIZE {
            let mut rest = vec![0u8; total - WAL_RECORD_HEADER_SIZE];
            if !self.try_read_exact(&mut rest)? {
                // The record is torn: the header was written but the payload
                // could not be fully read. Treat this as the end of the WAL and
                // roll back the read position to the start of the torn record.
                self.current_lsn = start_lsn;
                return Ok(None);
            }
            buf.extend_from_slice(&rest);
        }

        let (record, consumed) = WalRecord::decode(&buf)?;
        debug_assert_eq!(consumed, total);
        // current_lsn was already advanced by try_read_exact to the byte
        // immediately following the record.
        Ok(Some(record))
    }

    /// Return an iterator over records starting from the current position.
    pub fn records(&mut self) -> WalRecordIter<'_> {
        WalRecordIter { reader: self }
    }

    // TODO(Phase 3): implement `tail_follow(start_lsn: Lsn)` returning an async
    // `Stream<Item = Result<WalRecord>>` for streaming WAL to replicas /
    // indexers. M1 only needs one-shot recovery reads.

    fn open_segment_file(wal_dir: &Path, segment_id: u64) -> Result<File> {
        let path = wal_dir.join(wal_filename(segment_id));
        OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(StorageError::Io)
    }

    /// Fill `buf` starting from `self.current_lsn`, automatically opening the
    /// next segment file when the current segment boundary is crossed.
    ///
    /// Returns `Ok(false)` when the WAL ends before `buf` can be completely
    /// filled (missing segment file or EOF). This is treated as a clean
    /// end-of-WAL, which recovery code will use to discard any torn/partial
    /// record. Returns an error only on I/O failures.
    fn try_read_exact(&mut self, buf: &mut [u8]) -> Result<bool> {
        let mut read = 0;
        while read < buf.len() {
            let pos = Lsn(self.current_lsn.0 + read as u64);
            let segment_id = pos.segment_id(self.segment_size);
            let offset = pos.segment_offset(self.segment_size);

            if segment_id != self.current_segment_id {
                let path = self.wal_dir.join(wal_filename(segment_id));
                if !path.exists() {
                    return Ok(false);
                }
                self.current_file = Self::open_segment_file(&self.wal_dir, segment_id)?;
                self.current_segment_id = segment_id;
            }

            let remaining_in_segment = (self.segment_size - offset) as usize;
            let chunk = std::cmp::min(remaining_in_segment, buf.len() - read);
            self.current_file
                .seek(SeekFrom::Start(offset))
                .map_err(StorageError::Io)?;
            let n = self
                .current_file
                .read(&mut buf[read..read + chunk])
                .map_err(StorageError::Io)?;
            // `read()` may return fewer than `chunk` bytes. The loop will retry
            // within the same segment; the seek at the top of each iteration
            // positions the fd cursor at the correct offset before the next
            // read. If this is ever refactored to use `pread()`, the retry
            // path must account for the new offset.
            if n == 0 {
                return Ok(false);
            }
            read += n;
        }

        self.current_lsn = Lsn(self.current_lsn.0 + buf.len() as u64);
        Ok(true)
    }

    /// Like [`Self::try_read_exact`] but reads from `pos` without advancing
    /// `self.current_lsn`. Used by the zero-hole forward probe — the reader
    /// peeks ahead while keeping its position at the start of the candidate
    /// hole so that `next_record` can roll back cleanly.
    fn try_read_exact_at(&mut self, pos: u64, buf: &mut [u8]) -> Result<bool> {
        let mut read = 0;
        while read < buf.len() {
            let p = Lsn(pos + read as u64);
            let segment_id = p.segment_id(self.segment_size);
            let offset = p.segment_offset(self.segment_size);

            if segment_id != self.current_segment_id {
                let path = self.wal_dir.join(wal_filename(segment_id));
                if !path.exists() {
                    return Ok(false);
                }
                self.current_file = Self::open_segment_file(&self.wal_dir, segment_id)?;
                self.current_segment_id = segment_id;
            }

            let remaining_in_segment = (self.segment_size - offset) as usize;
            let chunk = std::cmp::min(remaining_in_segment, buf.len() - read);
            self.current_file
                .seek(SeekFrom::Start(offset))
                .map_err(StorageError::Io)?;
            let n = self
                .current_file
                .read(&mut buf[read..read + chunk])
                .map_err(StorageError::Io)?;
            if n == 0 {
                return Ok(false);
            }
            read += n;
        }
        Ok(true)
    }
}

/// Iterator over WAL records.
pub struct WalRecordIter<'a> {
    reader: &'a mut WalReader,
}

impl Iterator for WalRecordIter<'_> {
    type Item = Result<WalRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.next_record() {
            Ok(None) => None,
            Ok(Some(record)) => Some(Ok(record)),
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    use crate::config::StorageConfig;
    use crate::types::PageId;
    use crate::wal::record::WalRecordType;
    use crate::wal::writer::WalWriter;
    use tempfile::TempDir;

    fn writer_config(tmp: &TempDir) -> StorageConfig {
        let mut cfg = StorageConfig::new(tmp.path());
        cfg.wal_group_commit_timeout_ms = 1;
        cfg.wal_group_commit_batch_size = 1;
        cfg.wal_segment_size = 1024;
        cfg
    }

    #[test]
    fn read_single_record() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();
        let lsn = writer
            .append(WalRecord::page_alloc(PageId(42)).unwrap())
            .unwrap();

        let mut reader = WalReader::open(tmp.path().join("wal"), cfg.wal_segment_size).unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(record.record_type, WalRecordType::PageAlloc);
        assert_eq!(record.lsn, lsn);
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn read_multiple_records_preserves_order() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        let count: usize = 100;
        let mut lsns = Vec::new();
        for i in 0..count {
            let lsn = writer
                .append(WalRecord::page_alloc(PageId(i as u64 + 1)).unwrap())
                .unwrap();
            lsns.push(lsn);
        }

        let mut reader = WalReader::open(tmp.path().join("wal"), cfg.wal_segment_size).unwrap();
        let mut read = 0;
        while let Some(record) = reader.next_record().unwrap() {
            assert_eq!(record.lsn, lsns[read]);
            read += 1;
        }
        assert_eq!(read, count);
    }

    #[test]
    fn read_across_segment_boundary() {
        let tmp = TempDir::new().unwrap();
        let mut cfg = writer_config(&tmp);
        cfg.wal_segment_size = 256;
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        // Write enough records to cross into a second segment.
        let n: usize = 20;
        for i in 0..n {
            writer
                .append(WalRecord::page_alloc(PageId(i as u64 + 1)).unwrap())
                .unwrap();
        }

        let mut reader = WalReader::open(tmp.path().join("wal"), cfg.wal_segment_size).unwrap();
        let mut count = 0;
        while reader.next_record().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, n);
    }

    #[test]
    fn read_from_start_lsn() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        let lsn1 = writer
            .append(WalRecord::page_alloc(PageId(1)).unwrap())
            .unwrap();
        writer
            .append(WalRecord::page_alloc(PageId(2)).unwrap())
            .unwrap();

        let mut reader =
            WalReader::open_at(tmp.path().join("wal"), cfg.wal_segment_size, lsn1).unwrap();
        let record = reader.next_record().unwrap().unwrap();
        assert_eq!(record.lsn, lsn1);
        assert!(reader.next_record().unwrap().is_some());
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn read_rejects_corrupted_crc() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();
        let lsn = writer
            .append(WalRecord::page_alloc(PageId(1)).unwrap())
            .unwrap();

        // Corrupt the first byte of the record (part of the LSN).
        let path = tmp.path().join("wal").join("wal-00000001.log");
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let offset = lsn.segment_offset(cfg.wal_segment_size);
        let mut buf = [0u8; 1];
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.read_exact(&mut buf).unwrap();
        buf[0] ^= 0xff;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&buf).unwrap();
        drop(file);

        let mut reader = WalReader::open(tmp.path().join("wal"), cfg.wal_segment_size).unwrap();
        assert!(reader.next_record().is_err());
    }

    #[test]
    fn read_empty_wal_returns_none() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        // Create the WAL directory and an empty segment file.
        let wal_dir = tmp.path().join("wal");
        std::fs::create_dir(&wal_dir).unwrap();
        File::create(wal_dir.join("wal-00000001.log")).unwrap();

        let mut reader = WalReader::open(&wal_dir, cfg.wal_segment_size).unwrap();
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn read_torn_payload_returns_none() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();
        let lsn = writer
            .append(WalRecord::full_page_image(PageId(1), vec![0xAB; 256]).unwrap())
            .unwrap();
        drop(writer);

        // Truncate the file so that the header is intact but the payload is
        // only partially present. This simulates a crash after the header was
        // written but before the full record was fsynced.
        let path = tmp.path().join("wal").join("wal-00000001.log");
        let record_start = lsn.segment_offset(cfg.wal_segment_size);
        let truncate_len = record_start + WAL_RECORD_HEADER_SIZE as u64 + 4;
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(truncate_len).unwrap();
        drop(file);

        let mut reader = WalReader::open(tmp.path().join("wal"), cfg.wal_segment_size).unwrap();
        assert!(reader.next_record().unwrap().is_none());
        // The torn record should be discarded; current_lsn rolls back to the
        // start of the torn record.
        assert_eq!(reader.current_lsn(), lsn);
    }

    #[test]
    fn read_torn_header_returns_none() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();
        let lsn = writer
            .append(WalRecord::page_alloc(PageId(1)).unwrap())
            .unwrap();
        drop(writer);

        // Truncate the file mid-header.
        let path = tmp.path().join("wal").join("wal-00000001.log");
        let record_start = lsn.segment_offset(cfg.wal_segment_size);
        let truncate_len = record_start + (WAL_RECORD_HEADER_SIZE as u64) / 2;
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(truncate_len).unwrap();
        drop(file);

        let mut reader = WalReader::open(tmp.path().join("wal"), cfg.wal_segment_size).unwrap();
        assert!(reader.next_record().unwrap().is_none());
        assert_eq!(reader.current_lsn(), lsn);
    }

    #[test]
    fn read_iterator_matches_next_record() {
        let tmp = TempDir::new().unwrap();
        let cfg = writer_config(&tmp);
        let writer = WalWriter::open(tmp.path(), &cfg).unwrap();

        for i in 0..5 {
            writer
                .append(WalRecord::page_alloc(PageId(i + 1)).unwrap())
                .unwrap();
        }

        let mut reader = WalReader::open(tmp.path().join("wal"), cfg.wal_segment_size).unwrap();
        let from_iter: Vec<_> = reader.records().map(|r| r.unwrap().record_type).collect();
        assert_eq!(from_iter, vec![WalRecordType::PageAlloc; 5]);
    }
}
