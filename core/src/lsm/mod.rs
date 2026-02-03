//! LSM Tree key-value store with duplicate key support.
//!
//! Architecture:
//! - MemTable: In-memory sorted structure for fast writes
//! - SSTable: Immutable on-disk sorted files
//! - WAL: Write-ahead log for durability
//! - Compaction: Background merging of SSTables

mod types;
mod memtable;
mod sstable;
mod wal;
mod iterator;
mod lsm;

pub use types::{Key, Value, Entry, SeqNum};
pub use memtable::MemTable;
pub use sstable::{SSTableWriter, SSTableReader};
pub use lsm::{LsmTree, LsmConfig, LsmStats};
pub use iterator::MergeIterator;
