//! Merge iterator for combining multiple sorted sources.
//!
//! Used for range scans that need to merge results from memtable and SSTables.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use super::types::Entry;

/// A wrapper for entries that implements reverse ordering for the min-heap.
struct HeapEntry {
    entry: Entry,
    source_idx: usize,
}

impl Eq for HeapEntry {}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.entry == other.entry
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap behavior
        // First compare by key (ascending), then by seq_num (descending)
        match other.entry.key.cmp(&self.entry.key) {
            Ordering::Equal => self.entry.seq_num.cmp(&other.entry.seq_num),
            ord => ord,
        }
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Merge iterator that combines multiple sorted iterators.
/// 
/// For duplicate keys, entries are returned in seq_num descending order
/// (newest first), which allows callers to implement different policies:
/// - Return only the latest value
/// - Return all values for a key
/// - Apply tombstone filtering
pub struct MergeIterator<I> {
    sources: Vec<I>,
    heap: BinaryHeap<HeapEntry>,
    initialized: bool,
}

impl<I> MergeIterator<I>
where
    I: Iterator<Item = Entry>,
{
    /// Create a new merge iterator from multiple sources.
    pub fn new(sources: Vec<I>) -> Self {
        Self {
            sources,
            heap: BinaryHeap::new(),
            initialized: false,
        }
    }

    fn initialize(&mut self) {
        if self.initialized {
            return;
        }
        self.initialized = true;

        // Prime the heap with one entry from each source
        for (idx, source) in self.sources.iter_mut().enumerate() {
            if let Some(entry) = source.next() {
                self.heap.push(HeapEntry {
                    entry,
                    source_idx: idx,
                });
            }
        }
    }
}

impl<I> Iterator for MergeIterator<I>
where
    I: Iterator<Item = Entry>,
{
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        self.initialize();

        if let Some(heap_entry) = self.heap.pop() {
            // Replenish from the same source
            if let Some(next_entry) = self.sources[heap_entry.source_idx].next() {
                self.heap.push(HeapEntry {
                    entry: next_entry,
                    source_idx: heap_entry.source_idx,
                });
            }

            Some(heap_entry.entry)
        } else {
            None
        }
    }
}

/// Iterator adapter that filters out older versions of duplicate keys.
/// Only returns the entry with the highest seq_num for each key.
pub struct LatestVersionIterator<I> {
    inner: I,
    last_key: Option<super::types::Key>,
}

impl<I> LatestVersionIterator<I>
where
    I: Iterator<Item = Entry>,
{
    pub fn new(inner: I) -> Self {
        Self {
            inner,
            last_key: None,
        }
    }
}

impl<I> Iterator for LatestVersionIterator<I>
where
    I: Iterator<Item = Entry>,
{
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next() {
                Some(entry) => {
                    // Skip if same key as last (we already returned the newer version)
                    if self.last_key.as_ref() == Some(&entry.key) {
                        continue;
                    }
                    self.last_key = Some(entry.key.clone());
                    return Some(entry);
                }
                None => return None,
            }
        }
    }
}

/// Iterator adapter that filters out tombstones.
pub struct LiveEntriesIterator<I> {
    inner: I,
}

impl<I> LiveEntriesIterator<I>
where
    I: Iterator<Item = Entry>,
{
    pub fn new(inner: I) -> Self {
        Self { inner }
    }
}

impl<I> Iterator for LiveEntriesIterator<I>
where
    I: Iterator<Item = Entry>,
{
    type Item = Entry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next() {
                Some(entry) if entry.is_tombstone() => continue,
                other => return other,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lsm::types::{Key, Value};

    #[test]
    fn test_merge_iterator_basic() {
        let source1 = vec![
            Entry::put(Key::from("a"), 1, Value::from("v1")),
            Entry::put(Key::from("c"), 3, Value::from("v3")),
        ];
        let source2 = vec![
            Entry::put(Key::from("b"), 2, Value::from("v2")),
            Entry::put(Key::from("d"), 4, Value::from("v4")),
        ];

        let merged: Vec<_> = MergeIterator::new(vec![source1.into_iter(), source2.into_iter()])
            .collect();

        let keys: Vec<_> = merged.iter().map(|e| e.key.as_bytes()).collect();
        assert_eq!(keys, vec![b"a", b"b", b"c", b"d"]);
    }

    #[test]
    fn test_merge_iterator_duplicates() {
        let source1 = vec![
            Entry::put(Key::from("key"), 1, Value::from("old")),
        ];
        let source2 = vec![
            Entry::put(Key::from("key"), 2, Value::from("new")),
        ];

        let merged: Vec<_> = MergeIterator::new(vec![source1.into_iter(), source2.into_iter()])
            .collect();

        // Both entries should be present, newer first
        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].seq_num, 2);
        assert_eq!(merged[1].seq_num, 1);
    }

    #[test]
    fn test_latest_version_iterator() {
        let entries = vec![
            Entry::put(Key::from("a"), 2, Value::from("new")),
            Entry::put(Key::from("a"), 1, Value::from("old")),
            Entry::put(Key::from("b"), 3, Value::from("val")),
        ];

        let latest: Vec<_> = LatestVersionIterator::new(entries.into_iter()).collect();

        assert_eq!(latest.len(), 2);
        assert_eq!(latest[0].key.as_bytes(), b"a");
        assert_eq!(latest[0].seq_num, 2);
        assert_eq!(latest[1].key.as_bytes(), b"b");
    }

    #[test]
    fn test_live_entries_iterator() {
        let entries = vec![
            Entry::put(Key::from("a"), 1, Value::from("val")),
            Entry::delete(Key::from("b"), 2),
            Entry::put(Key::from("c"), 3, Value::from("val")),
        ];

        let live: Vec<_> = LiveEntriesIterator::new(entries.into_iter()).collect();

        assert_eq!(live.len(), 2);
        assert_eq!(live[0].key.as_bytes(), b"a");
        assert_eq!(live[1].key.as_bytes(), b"c");
    }
}
