//! pg_rust storage engine — Phase 1 M1.
//!
//! This crate implements the physical storage layer:
//! - Page allocation
//! - Write-ahead logging (WAL)
//! - Buffer pool
//! - LSN clock
//! - Checkpoint / recovery
//!
//! It intentionally does **not** expose a generic "File Manager" abstraction.
//! File-management responsibilities live inside the components that own the
//! crash-safety invariants.

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod config;
pub mod error;
pub mod freelist_meta;
pub mod io;
pub mod lsn_clock;
pub mod superblock;
pub mod types;
pub mod wal;

// Modules introduced in later stages (M1 E–K):
// pub mod buffer_pool;
// pub mod page_allocator;
// pub mod checkpoint;

/// Initialize a global `tracing` subscriber for tests.
///
/// Applications using `pg-storage` as a library should set up their own
/// subscriber instead of calling this function.
#[cfg(test)]
pub(crate) fn init_test_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logging_initializes_without_panic() {
        // Smoke test that ensures the test workspace builds and the tracing
        // subscriber can be initialized.
        init_test_logging();
    }
}
