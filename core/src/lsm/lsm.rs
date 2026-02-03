//! LSM Tree - Main coordinator for the key-value store.
//!
//! Manages memtable lifecycle, SSTable creation, and read path.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::bufferpool::BufferPool;

use super::iterator::{LatestVersionIterator, LiveEntriesIterator, MergeIterator};
use super::memtable::MemTable;
use super::sstable::{SSTableReader, SSTableWriter};
use super::types::{Entry, Key, SeqNum, Value};
use super::wal::{delete_wal, Wal, WalReader};

/// Configuration for the LSM tree.
#[derive(Clone)]
pub struct LsmConfig {
    /// Maximum memtable size before flushing to SSTable.
    pub memtable_size_threshold: usize,
    /// Directory for data files.
    pub data_dir: PathBuf,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            memtable_size_threshold: 4 * 1024 * 1024, // 4MB
            data_dir: PathBuf::from("./data"),
        }
    }
}

/// LSM Tree key-value store with duplicate key support.
pub struct LsmTree {
    config: LsmConfig,
    buffer_pool: Arc<BufferPool>,
    
    /// Active memtable for writes.
    memtable: RwLock<MemTable>,
    
    /// Write-ahead log.
    wal: RwLock<Wal>,
    
    /// Immutable SSTables (newest first).
    sstables: RwLock<Vec<SSTableReader>>,
    
    /// Next SSTable file ID.
    next_sstable_id: AtomicU64,
}

impl LsmTree {
    /// Create a new LSM tree or open an existing one.
    pub fn open(config: LsmConfig) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(&config.data_dir)?;
        
        let page_store_dir = config.data_dir.join("pages");
        std::fs::create_dir_all(&page_store_dir)?;
        
        let buffer_pool = Arc::new(BufferPool::new(
            page_store_dir.to_string_lossy().to_string()
        )?);

        // Open WAL
        let wal_path = config.data_dir.join("wal.log");
        let wal = Wal::open(&wal_path)?;

        // Recover memtable from WAL if exists
        let memtable = Self::recover_memtable(&wal_path)?;

        // Load existing SSTables
        let (sstables, next_id) = Self::load_sstables(&config.data_dir, buffer_pool.clone())?;

        Ok(Self {
            config,
            buffer_pool,
            memtable: RwLock::new(memtable),
            wal: RwLock::new(wal),
            sstables: RwLock::new(sstables),
            next_sstable_id: AtomicU64::new(next_id),
        })
    }

    fn recover_memtable(wal_path: &PathBuf) -> Result<MemTable, std::io::Error> {
        let mut memtable = MemTable::new();
        
        if wal_path.exists() {
            match WalReader::open(wal_path) {
                Ok(mut reader) => {
                    let entries = reader.read_all()?;
                    for entry in entries {
                        if let Some(value) = entry.value {
                            memtable.put_with_seq(entry.key, value, entry.seq_num);
                        } else {
                            memtable.delete_with_seq(entry.key, entry.seq_num);
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        
        Ok(memtable)
    }

    fn load_sstables(
        data_dir: &PathBuf,
        buffer_pool: Arc<BufferPool>,
    ) -> Result<(Vec<SSTableReader>, u64), std::io::Error> {
        let mut sstables = Vec::new();
        let mut max_id = 0u64;

        // Look for SSTable metadata files
        let manifest_path = data_dir.join("manifest");
        if manifest_path.exists() {
            let manifest_content = std::fs::read_to_string(&manifest_path)?;
            for line in manifest_content.lines() {
                if let Ok(id) = line.parse::<u64>() {
                    match SSTableReader::open(buffer_pool.clone(), id) {
                        Ok(reader) => {
                            max_id = max_id.max(id);
                            sstables.push(reader);
                        }
                        Err(e) => {
                            eprintln!("Warning: Failed to open SSTable {}: {}", id, e);
                        }
                    }
                }
            }
        }

        // Sort by ID descending (newest first)
        sstables.sort_by(|a, b| b.meta.id.cmp(&a.meta.id));

        Ok((sstables, max_id + 1))
    }

    fn save_manifest(&self) -> Result<(), std::io::Error> {
        let manifest_path = self.config.data_dir.join("manifest");
        let sstables = self.sstables.read().unwrap();
        let content: String = sstables
            .iter()
            .map(|s| s.meta.id.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(manifest_path, content)
    }

    /// Put a key-value pair.
    pub fn put(&self, key: Key, value: Value) -> Result<SeqNum, std::io::Error> {
        let seq_num;
        {
            let mut memtable = self.memtable.write().unwrap();
            
            // Allocate seq_num via put() which handles incrementing
            seq_num = memtable.put(key.clone(), value.clone());
        }

        // Log to WAL (after successful memtable write for seq_num, but we flush to ensure durability)
        {
            let mut wal = self.wal.write().unwrap();
            wal.log_put(&key, &value, seq_num)?;
        }

        // Check if we need to flush
        self.maybe_flush()?;

        Ok(seq_num)
    }

    /// Delete a key.
    pub fn delete(&self, key: Key) -> Result<SeqNum, std::io::Error> {
        let seq_num;
        {
            let mut memtable = self.memtable.write().unwrap();
            seq_num = memtable.delete(key.clone());
        }

        {
            let mut wal = self.wal.write().unwrap();
            wal.log_delete(&key, seq_num)?;
        }

        self.maybe_flush()?;

        Ok(seq_num)
    }

    /// Get the latest value for a key.
    /// Returns None if not found or deleted.
    pub fn get(&self, key: &Key) -> Result<Option<Value>, std::io::Error> {
        // Check memtable first
        {
            let memtable = self.memtable.read().unwrap();
            if let Some(value_opt) = memtable.get(key) {
                return Ok(value_opt.cloned());
            }
        }

        // Check SSTables (newest to oldest)
        let sstables = self.sstables.read().unwrap();
        for sstable in sstables.iter() {
            let entries = sstable.get(key)?;
            if !entries.is_empty() {
                // Return the newest entry's value (first in the list)
                return Ok(entries[0].value.clone());
            }
        }

        Ok(None)
    }

    /// Get all values for a key (for duplicate key support).
    /// Returns entries in seq_num descending order (newest first).
    pub fn get_all(&self, key: &Key) -> Result<Vec<Entry>, std::io::Error> {
        let mut all_entries = Vec::new();

        // Get from memtable
        {
            let memtable = self.memtable.read().unwrap();
            for (seq_num, value_opt) in memtable.get_all(key) {
                let entry = match value_opt {
                    Some(v) => Entry::put(key.clone(), seq_num, v.clone()),
                    None => Entry::delete(key.clone(), seq_num),
                };
                all_entries.push(entry);
            }
        }

        // Get from SSTables
        let sstables = self.sstables.read().unwrap();
        for sstable in sstables.iter() {
            let entries = sstable.get(key)?;
            all_entries.extend(entries);
        }

        // Sort by seq_num descending
        all_entries.sort_by(|a, b| b.seq_num.cmp(&a.seq_num));

        Ok(all_entries)
    }

    /// Scan all entries in sorted order.
    /// Returns entries merged from memtable and all SSTables.
    pub fn scan(&self) -> Result<impl Iterator<Item = Entry>, std::io::Error> {
        let mut sources: Vec<Box<dyn Iterator<Item = Entry>>> = Vec::new();

        // Add memtable entries
        {
            let memtable = self.memtable.read().unwrap();
            let entries: Vec<Entry> = memtable.iter().collect();
            sources.push(Box::new(entries.into_iter()));
        }

        // Add SSTable entries
        let sstables = self.sstables.read().unwrap();
        for sstable in sstables.iter() {
            let iter = sstable.iter().filter_map(|r| r.ok());
            sources.push(Box::new(iter.collect::<Vec<_>>().into_iter()));
        }

        Ok(MergeIterator::new(sources))
    }

    /// Scan with only latest versions (no duplicates).
    pub fn scan_latest(&self) -> Result<impl Iterator<Item = Entry>, std::io::Error> {
        Ok(LatestVersionIterator::new(self.scan()?))
    }

    /// Scan with only live entries (no tombstones).
    pub fn scan_live(&self) -> Result<impl Iterator<Item = Entry>, std::io::Error> {
        Ok(LiveEntriesIterator::new(LatestVersionIterator::new(self.scan()?)))
    }

    fn maybe_flush(&self) -> Result<(), std::io::Error> {
        let should_flush = {
            let memtable = self.memtable.read().unwrap();
            memtable.size_bytes() >= self.config.memtable_size_threshold
        };

        if should_flush {
            self.flush()?;
        }

        Ok(())
    }

    /// Force flush the memtable to an SSTable.
    pub fn flush(&self) -> Result<(), std::io::Error> {
        let entries: Vec<Entry>;
        let wal_path: String;
        
        {
            let memtable = self.memtable.read().unwrap();
            if memtable.is_empty() {
                return Ok(());
            }
            entries = memtable.iter().collect();
            
            let wal = self.wal.read().unwrap();
            wal_path = wal.path().to_string();
        }

        // Create new SSTable
        let sstable_id = self.next_sstable_id.fetch_add(1, Ordering::SeqCst);
        
        {
            let mut writer = SSTableWriter::new(&self.buffer_pool, sstable_id)?;
            for entry in &entries {
                writer.write_entry(entry)?;
            }
            writer.finish()?;
        }

        // Flush buffer pool to ensure SSTable is persisted
        self.buffer_pool.flush()?;

        // Open the new SSTable for reading
        let reader = SSTableReader::open(self.buffer_pool.clone(), sstable_id)?;

        // Add to SSTables list
        {
            let mut sstables = self.sstables.write().unwrap();
            sstables.insert(0, reader); // Insert at front (newest)
        }

        // Clear memtable and reset WAL
        {
            let mut memtable = self.memtable.write().unwrap();
            let seq_num = memtable.current_seq_num();
            memtable.clear();
            // Preserve sequence number across flushes
            *memtable = MemTable::with_seq_num(seq_num);
        }

        // Delete old WAL and create new one
        drop(self.wal.write().unwrap());
        let _ = delete_wal(&wal_path);
        
        let new_wal_path = self.config.data_dir.join("wal.log");
        let new_wal = Wal::open(&new_wal_path)?;
        *self.wal.write().unwrap() = new_wal;

        // Update manifest
        self.save_manifest()?;

        Ok(())
    }

    /// Get statistics about the LSM tree.
    pub fn stats(&self) -> LsmStats {
        let memtable = self.memtable.read().unwrap();
        let sstables = self.sstables.read().unwrap();

        LsmStats {
            memtable_entries: memtable.len(),
            memtable_size_bytes: memtable.size_bytes(),
            sstable_count: sstables.len(),
            total_entries: sstables.iter().map(|s| s.meta.entry_count).sum::<u64>() as usize
                + memtable.len(),
        }
    }
}

/// Statistics about the LSM tree.
#[derive(Debug, Clone)]
pub struct LsmStats {
    pub memtable_entries: usize,
    pub memtable_size_bytes: usize,
    pub sstable_count: usize,
    pub total_entries: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn get_temp_dir() -> PathBuf {
        let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        PathBuf::from(format!("/tmp/thordb_lsm_test_{}", since_epoch.as_nanos()))
    }

    #[test]
    fn test_basic_put_get() {
        let dir = get_temp_dir();
        let config = LsmConfig {
            data_dir: dir.clone(),
            ..Default::default()
        };

        let lsm = LsmTree::open(config).unwrap();

        lsm.put(Key::from("key1"), Value::from("value1")).unwrap();
        lsm.put(Key::from("key2"), Value::from("value2")).unwrap();

        assert_eq!(
            lsm.get(&Key::from("key1")).unwrap().unwrap().as_bytes(),
            b"value1"
        );
        assert_eq!(
            lsm.get(&Key::from("key2")).unwrap().unwrap().as_bytes(),
            b"value2"
        );
        assert!(lsm.get(&Key::from("key3")).unwrap().is_none());

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_overwrite() {
        let dir = get_temp_dir();
        let config = LsmConfig {
            data_dir: dir.clone(),
            ..Default::default()
        };

        let lsm = LsmTree::open(config).unwrap();

        lsm.put(Key::from("key"), Value::from("v1")).unwrap();
        lsm.put(Key::from("key"), Value::from("v2")).unwrap();

        // Get returns latest
        assert_eq!(
            lsm.get(&Key::from("key")).unwrap().unwrap().as_bytes(),
            b"v2"
        );

        // Get all returns both
        let all = lsm.get_all(&Key::from("key")).unwrap();
        assert_eq!(all.len(), 2);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_delete() {
        let dir = get_temp_dir();
        let config = LsmConfig {
            data_dir: dir.clone(),
            ..Default::default()
        };

        let lsm = LsmTree::open(config).unwrap();

        lsm.put(Key::from("key"), Value::from("value")).unwrap();
        lsm.delete(Key::from("key")).unwrap();

        assert!(lsm.get(&Key::from("key")).unwrap().is_none());

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_flush_and_recover() {
        let dir = get_temp_dir();
        let config = LsmConfig {
            data_dir: dir.clone(),
            memtable_size_threshold: 100, // Small threshold for testing
            ..Default::default()
        };

        // Write and flush
        {
            let lsm = LsmTree::open(config.clone()).unwrap();
            lsm.put(Key::from("key1"), Value::from("value1")).unwrap();
            lsm.put(Key::from("key2"), Value::from("value2")).unwrap();
            lsm.flush().unwrap();
        }

        // Reopen and verify
        {
            let lsm = LsmTree::open(config).unwrap();
            assert_eq!(
                lsm.get(&Key::from("key1")).unwrap().unwrap().as_bytes(),
                b"value1"
            );
            assert_eq!(
                lsm.get(&Key::from("key2")).unwrap().unwrap().as_bytes(),
                b"value2"
            );
        }

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_scan() {
        let dir = get_temp_dir();
        let config = LsmConfig {
            data_dir: dir.clone(),
            ..Default::default()
        };

        let lsm = LsmTree::open(config).unwrap();

        lsm.put(Key::from("c"), Value::from("3")).unwrap();
        lsm.put(Key::from("a"), Value::from("1")).unwrap();
        lsm.put(Key::from("b"), Value::from("2")).unwrap();

        let entries: Vec<_> = lsm.scan_live().unwrap().collect();
        let keys: Vec<_> = entries.iter().map(|e| e.key.as_bytes()).collect();

        assert_eq!(keys, vec![b"a", b"b", b"c"]);

        let _ = std::fs::remove_dir_all(dir);
    }
}
