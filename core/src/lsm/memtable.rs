//! In-memory sorted table for fast writes.
//!
//! The MemTable stores entries sorted by (key, seq_num desc) using a BTreeMap.
//! This allows efficient point lookups and range scans.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use super::types::{Entry, Key, SeqNum, Value};

/// In-memory sorted table.
/// 
/// Entries are stored sorted by (key, reverse seq_num) so that:
/// - Keys are in ascending order
/// - For the same key, newer entries (higher seq_num) come first
pub struct MemTable {
    /// Entries stored as (key, seq_num) -> Option<Value>
    /// We use reverse seq_num ordering within the same key.
    entries: BTreeMap<(Key, std::cmp::Reverse<SeqNum>), Option<Value>>,
    /// Current size in bytes (approximate).
    size_bytes: usize,
    /// Sequence number generator.
    next_seq_num: AtomicU64,
}

impl MemTable {
    /// Create a new empty memtable.
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            size_bytes: 0,
            next_seq_num: AtomicU64::new(1),
        }
    }

    /// Create a memtable starting from a specific sequence number.
    pub fn with_seq_num(start_seq_num: SeqNum) -> Self {
        Self {
            entries: BTreeMap::new(),
            size_bytes: 0,
            next_seq_num: AtomicU64::new(start_seq_num),
        }
    }

    /// Get the next sequence number and increment.
    fn alloc_seq_num(&self) -> SeqNum {
        self.next_seq_num.fetch_add(1, Ordering::SeqCst)
    }

    /// Current sequence number (for recovery).
    pub fn current_seq_num(&self) -> SeqNum {
        self.next_seq_num.load(Ordering::SeqCst)
    }

    /// Put a key-value pair. Returns the sequence number assigned.
    pub fn put(&mut self, key: Key, value: Value) -> SeqNum {
        let seq_num = self.alloc_seq_num();
        self.put_with_seq(key, value, seq_num);
        seq_num
    }

    /// Put with an explicit sequence number (used during WAL replay).
    pub fn put_with_seq(&mut self, key: Key, value: Value, seq_num: SeqNum) {
        let entry_size = key.len() + value.len() + 8 + 16; // approximate overhead
        self.size_bytes += entry_size;
        self.entries.insert(
            (key, std::cmp::Reverse(seq_num)),
            Some(value),
        );
    }

    /// Delete a key. Returns the sequence number assigned.
    pub fn delete(&mut self, key: Key) -> SeqNum {
        let seq_num = self.alloc_seq_num();
        self.delete_with_seq(key, seq_num);
        seq_num
    }

    /// Delete with an explicit sequence number (used during WAL replay).
    pub fn delete_with_seq(&mut self, key: Key, seq_num: SeqNum) {
        let entry_size = key.len() + 8 + 16; // approximate overhead
        self.size_bytes += entry_size;
        self.entries.insert(
            (key, std::cmp::Reverse(seq_num)),
            None, // tombstone
        );
    }

    /// Get the latest value for a key.
    /// Returns Some(Some(value)) if found, Some(None) if deleted (tombstone),
    /// or None if key never existed.
    pub fn get(&self, key: &Key) -> Option<Option<&Value>> {
        // Find the first entry with this key (which has the highest seq_num due to ordering)
        let start = (key.clone(), std::cmp::Reverse(SeqNum::MAX));
        let end = (key.clone(), std::cmp::Reverse(0));
        
        self.entries
            .range(start..=end)
            .next()
            .map(|(_, v)| v.as_ref())
    }

    /// Get all values for a key (for duplicate key support).
    /// Returns entries in seq_num descending order (newest first).
    pub fn get_all(&self, key: &Key) -> Vec<(SeqNum, Option<&Value>)> {
        let start = (key.clone(), std::cmp::Reverse(SeqNum::MAX));
        let end = (key.clone(), std::cmp::Reverse(0));
        
        self.entries
            .range(start..=end)
            .map(|((_, std::cmp::Reverse(seq)), v)| (*seq, v.as_ref()))
            .collect()
    }

    /// Iterate over all entries in sorted order.
    pub fn iter(&self) -> impl Iterator<Item = Entry> + '_ {
        self.entries.iter().map(|((key, std::cmp::Reverse(seq)), value)| {
            Entry {
                key: key.clone(),
                seq_num: *seq,
                value: value.clone(),
            }
        })
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Approximate size in bytes.
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// Clear the memtable.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.size_bytes = 0;
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_put_and_get() {
        let mut mem = MemTable::new();
        mem.put(Key::from("key1"), Value::from("value1"));
        mem.put(Key::from("key2"), Value::from("value2"));

        assert_eq!(
            mem.get(&Key::from("key1")).unwrap().unwrap().as_bytes(),
            b"value1"
        );
        assert_eq!(
            mem.get(&Key::from("key2")).unwrap().unwrap().as_bytes(),
            b"value2"
        );
        assert!(mem.get(&Key::from("key3")).is_none());
    }

    #[test]
    fn test_overwrite() {
        let mut mem = MemTable::new();
        mem.put(Key::from("key"), Value::from("v1"));
        mem.put(Key::from("key"), Value::from("v2"));

        // Latest value should be returned
        assert_eq!(
            mem.get(&Key::from("key")).unwrap().unwrap().as_bytes(),
            b"v2"
        );

        // Both values should be accessible via get_all
        let all = mem.get_all(&Key::from("key"));
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].1.unwrap().as_bytes(), b"v2"); // newer first
        assert_eq!(all[1].1.unwrap().as_bytes(), b"v1");
    }

    #[test]
    fn test_delete() {
        let mut mem = MemTable::new();
        mem.put(Key::from("key"), Value::from("value"));
        mem.delete(Key::from("key"));

        // Get should return tombstone
        assert!(mem.get(&Key::from("key")).unwrap().is_none());
    }

    #[test]
    fn test_iter_order() {
        let mut mem = MemTable::new();
        mem.put(Key::from("c"), Value::from("3"));
        mem.put(Key::from("a"), Value::from("1"));
        mem.put(Key::from("b"), Value::from("2"));

        let keys: Vec<_> = mem.iter().map(|e| e.key.0.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn test_duplicate_keys_ordering() {
        let mut mem = MemTable::new();
        let seq1 = mem.put(Key::from("key"), Value::from("first"));
        let seq2 = mem.put(Key::from("key"), Value::from("second"));

        assert!(seq2 > seq1);

        // Iteration should return same key twice, newer first
        let entries: Vec<_> = mem.iter().collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].seq_num, seq2);
        assert_eq!(entries[1].seq_num, seq1);
    }
}
