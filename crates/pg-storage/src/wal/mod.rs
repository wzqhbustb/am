//! Write-ahead log (WAL) subsystem.

pub mod segment;

pub use segment::{wal_filename, WalSegmentManager};
