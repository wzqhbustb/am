//! Storage-layer configuration.
//!
//! In M1 configuration is supplied through code-level defaults and a small set
//! of environment variables. File-based configuration (`pg_rust.conf`) will be
//! introduced in Phase 1 M3.

use std::path::PathBuf;

use crate::error::{Result, StorageError};
use crate::types::{PAGE_SIZE, WAL_SEGMENT_SIZE};

/// Default buffer pool capacity in bytes.
pub const DEFAULT_BUFFER_POOL_SIZE: usize = 128 * 1024 * 1024; // 128 MB

/// Default WAL group-commit timeout in milliseconds.
pub const DEFAULT_WAL_GROUP_COMMIT_TIMEOUT_MS: u64 = 2;

/// Default WAL group-commit batch size.
pub const DEFAULT_WAL_GROUP_COMMIT_BATCH_SIZE: usize = 64;

/// Default interval between automatic checkpoints in milliseconds.
///
/// A value of 0 disables automatic checkpoints; the caller must trigger them
/// manually via `CheckpointCoordinator::trigger_checkpoint`.
pub const DEFAULT_CHECKPOINT_INTERVAL_MS: u64 = 30_000;

/// Configuration for the storage engine.
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Directory where database files live.
    pub data_dir: PathBuf,

    /// WAL segment size in bytes.
    pub wal_segment_size: u64,

    /// Buffer pool capacity in bytes.
    pub buffer_pool_size: usize,

    /// Number of shards in the buffer pool page table.
    pub buffer_pool_shards: usize,

    /// Maximum number of WAL records to batch before fsync (group commit).
    pub wal_group_commit_batch_size: usize,

    /// Maximum milliseconds to wait before fsync (group commit).
    pub wal_group_commit_timeout_ms: u64,

    /// Interval between automatic checkpoints in milliseconds.
    ///
    /// Set to 0 to disable automatic checkpoints.
    pub checkpoint_interval_ms: u64,

    /// Page size in bytes.
    ///
    /// This is derived from the compile-time [`PAGE_SIZE`] constant and is
    /// kept private to prevent accidental mismatch between runtime config and
    /// the actual page format.
    page_size: usize,
}

impl StorageConfig {
    /// Return a default configuration rooted at `data_dir`.
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            page_size: PAGE_SIZE,
            wal_segment_size: WAL_SEGMENT_SIZE,
            buffer_pool_size: DEFAULT_BUFFER_POOL_SIZE,
            buffer_pool_shards: 256,
            wal_group_commit_batch_size: DEFAULT_WAL_GROUP_COMMIT_BATCH_SIZE,
            wal_group_commit_timeout_ms: DEFAULT_WAL_GROUP_COMMIT_TIMEOUT_MS,
            checkpoint_interval_ms: DEFAULT_CHECKPOINT_INTERVAL_MS,
        }
    }

    /// Return the compile-time page size.
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Validate the configuration.
    ///
    /// This should be called before starting the storage engine (e.g., in
    /// `Storage::open`). It catches obvious misconfigurations that would
    /// otherwise cause silent failures or panics deeper in the system.
    pub fn validate(&self) -> Result<()> {
        if self.page_size != PAGE_SIZE {
            return Err(StorageError::InvalidConfig(format!(
                "page_size {} does not match compile-time PAGE_SIZE {}",
                self.page_size, PAGE_SIZE
            )));
        }

        if self.wal_segment_size == 0 {
            return Err(StorageError::InvalidConfig(
                "wal_segment_size must be > 0".to_string(),
            ));
        }

        if self.buffer_pool_size == 0 || self.buffer_pool_size % self.page_size != 0 {
            return Err(StorageError::InvalidConfig(format!(
                "buffer_pool_size {} must be a positive multiple of page_size {}",
                self.buffer_pool_size, self.page_size
            )));
        }

        if self.buffer_pool_shards == 0 {
            return Err(StorageError::InvalidConfig(
                "buffer_pool_shards must be > 0".to_string(),
            ));
        }

        if self.wal_group_commit_batch_size == 0 {
            return Err(StorageError::InvalidConfig(
                "wal_group_commit_batch_size must be > 0".to_string(),
            ));
        }

        if self.wal_group_commit_timeout_ms == 0 {
            return Err(StorageError::InvalidConfig(
                "wal_group_commit_timeout_ms must be > 0".to_string(),
            ));
        }

        Ok(())
    }

    /// Read a minimal set of overrides from the environment.
    ///
    /// Recognized variables:
    /// - `PG_RUST_DATA_DIR`
    /// - `PG_RUST_BUFFER_POOL_SIZE`
    /// - `PG_RUST_BP_SHARDS`
    /// - `PG_RUST_WAL_TIMEOUT_MS`
    /// - `PG_RUST_WAL_BATCH_SIZE`
    /// - `PG_RUST_CHECKPOINT_INTERVAL_MS` (0 disables automatic checkpoints)
    pub fn from_env() -> Self {
        let data_dir = std::env::var("PG_RUST_DATA_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|_| PathBuf::from("pg_rust_data"));

        let mut cfg = Self::new(data_dir);

        if let Ok(size) = std::env::var("PG_RUST_BUFFER_POOL_SIZE") {
            match size.parse::<usize>() {
                Ok(size) => cfg.buffer_pool_size = size,
                Err(e) => tracing::warn!(
                    value = %size,
                    error = %e,
                    "PG_RUST_BUFFER_POOL_SIZE is not a valid usize; using default"
                ),
            }
        }

        if let Ok(shards) = std::env::var("PG_RUST_BP_SHARDS") {
            match shards.parse::<usize>() {
                Ok(shards) => cfg.buffer_pool_shards = shards,
                Err(e) => tracing::warn!(
                    value = %shards,
                    error = %e,
                    "PG_RUST_BP_SHARDS is not a valid usize; using default"
                ),
            }
        }

        if let Ok(timeout) = std::env::var("PG_RUST_WAL_TIMEOUT_MS") {
            match timeout.parse::<u64>() {
                Ok(timeout) => cfg.wal_group_commit_timeout_ms = timeout,
                Err(e) => tracing::warn!(
                    value = %timeout,
                    error = %e,
                    "PG_RUST_WAL_TIMEOUT_MS is not a valid u64; using default"
                ),
            }
        }

        if let Ok(batch) = std::env::var("PG_RUST_WAL_BATCH_SIZE") {
            match batch.parse::<usize>() {
                Ok(batch) => cfg.wal_group_commit_batch_size = batch,
                Err(e) => tracing::warn!(
                    value = %batch,
                    error = %e,
                    "PG_RUST_WAL_BATCH_SIZE is not a valid usize; using default"
                ),
            }
        }

        if let Ok(interval) = std::env::var("PG_RUST_CHECKPOINT_INTERVAL_MS") {
            match interval.parse::<u64>() {
                Ok(interval) => cfg.checkpoint_interval_ms = interval,
                Err(e) => tracing::warn!(
                    value = %interval,
                    error = %e,
                    "PG_RUST_CHECKPOINT_INTERVAL_MS is not a valid u64; using default"
                ),
            }
        }

        cfg
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self::new("pg_rust_data")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_uses_expected_constants() {
        let cfg = StorageConfig::default();
        assert_eq!(cfg.page_size(), PAGE_SIZE);
        assert_eq!(cfg.wal_segment_size, WAL_SEGMENT_SIZE);
        assert_eq!(cfg.buffer_pool_size, DEFAULT_BUFFER_POOL_SIZE);
        assert_eq!(cfg.buffer_pool_shards, 256);
        assert_eq!(cfg.checkpoint_interval_ms, DEFAULT_CHECKPOINT_INTERVAL_MS);
    }

    #[test]
    fn default_config_validates() {
        let cfg = StorageConfig::default();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn invalid_buffer_pool_size_fails_validation() {
        let cfg = StorageConfig {
            buffer_pool_size: 100, // not a multiple of page_size
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn zero_shards_fails_validation() {
        let cfg = StorageConfig {
            buffer_pool_shards: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn zero_group_commit_timeout_fails_validation() {
        let cfg = StorageConfig {
            wal_group_commit_timeout_ms: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn zero_batch_size_fails_validation() {
        let cfg = StorageConfig {
            wal_group_commit_batch_size: 0,
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }
}
