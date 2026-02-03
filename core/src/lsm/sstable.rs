//! SSTable (Sorted String Table) - immutable on-disk sorted files.
//!
//! Uses SerialPages for storage. Each SSTable consists of:
//! - Data pages: Sorted entries
//! - Metadata: Entry count, min/max keys, page range

use std::sync::Arc;

use crate::bufferpool::{BufferPool, PageAddr};
use crate::page::{Page, PageMut, PageRead};
use crate::tuple::tuple::{Tuple, TupleOnDisk};
use crate::tuple::types::TupleValue;

use super::types::{Entry, Key, SeqNum};

/// Metadata for an SSTable stored in the first page.
#[derive(Clone, Debug)]
pub struct SSTableMeta {
    /// Unique identifier for this SSTable.
    pub id: u64,
    /// Number of entries in the table.
    pub entry_count: u64,
    /// First data page (after metadata page).
    pub start_page: u64,
    /// Last data page (inclusive).
    pub end_page: u64,
    /// Minimum key in the table.
    pub min_key: Key,
    /// Maximum key in the table.
    pub max_key: Key,
    /// Minimum sequence number.
    pub min_seq: SeqNum,
    /// Maximum sequence number.
    pub max_seq: SeqNum,
}

/// Writer for creating an SSTable.
pub struct SSTableWriter<'a> {
    buffer_pool: &'a BufferPool,
    file_id: u64,
    current_page: u64,
    current_page_mut: PageMut<'a>,
    entry_count: u64,
    min_key: Option<Key>,
    max_key: Option<Key>,
    min_seq: SeqNum,
    max_seq: SeqNum,
}

impl<'a> SSTableWriter<'a> {
    /// Create a new SSTable writer.
    /// `file_id` is used for PageAddr file_id.
    /// Page 0 is reserved for metadata, data starts at page 1.
    pub fn new(buffer_pool: &'a BufferPool, file_id: u64) -> Result<Self, std::io::Error> {
        // Start writing at page 1 (page 0 is for metadata)
        let page_addr = PageAddr::new(file_id, 1);
        let current_page_mut = PageMut::open(buffer_pool, page_addr)?;

        Ok(Self {
            buffer_pool,
            file_id,
            current_page: 1,
            current_page_mut,
            entry_count: 0,
            min_key: None,
            max_key: None,
            min_seq: SeqNum::MAX,
            max_seq: 0,
        })
    }

    /// Write an entry to the SSTable.
    pub fn write_entry(&mut self, entry: &Entry) -> Result<(), std::io::Error> {
        // Serialize entry
        let mut entry_bytes = Vec::new();
        entry.write_to(&mut entry_bytes)?;

        // Create a tuple with the serialized entry as VarBytes
        let tuple = Tuple::new(vec![TupleValue::VarBytes(&entry_bytes)]);

        // Check if we need a new page
        if !self.current_page_mut.has_space_for_cell(tuple.len())? {
            self.current_page += 1;
            let page_addr = PageAddr::new(self.file_id, self.current_page);
            self.current_page_mut = PageMut::open(self.buffer_pool, page_addr)?;
        }

        // Write the tuple
        let cell_buffer = self.current_page_mut.allocate_cell(tuple.len())?;
        let mut cursor = std::io::Cursor::new(cell_buffer);
        tuple.write_to_stream(&mut cursor)?;

        // Update metadata
        self.entry_count += 1;
        if self.min_key.is_none() {
            self.min_key = Some(entry.key.clone());
        }
        self.max_key = Some(entry.key.clone());
        self.min_seq = self.min_seq.min(entry.seq_num);
        self.max_seq = self.max_seq.max(entry.seq_num);

        Ok(())
    }

    /// Finish writing and return metadata.
    pub fn finish(self) -> Result<SSTableMeta, std::io::Error> {
        let min_key = self.min_key.clone().unwrap_or_else(|| Key::new(vec![]));
        let max_key = self.max_key.clone().unwrap_or_else(|| Key::new(vec![]));
        
        let meta = SSTableMeta {
            id: self.file_id,
            entry_count: self.entry_count,
            start_page: 1,
            end_page: self.current_page,
            min_key,
            max_key,
            min_seq: if self.min_seq == SeqNum::MAX { 0 } else { self.min_seq },
            max_seq: self.max_seq,
        };

        // Write metadata to page 0
        self.write_metadata(&meta)?;

        Ok(meta)
    }

    fn write_metadata(&self, meta: &SSTableMeta) -> Result<(), std::io::Error> {
        let page_addr = PageAddr::new(self.file_id, 0);
        let mut meta_page = PageMut::open(self.buffer_pool, page_addr)?;

        // Serialize metadata as a tuple
        let mut meta_bytes = Vec::new();
        meta_bytes.extend_from_slice(&meta.id.to_le_bytes());
        meta_bytes.extend_from_slice(&meta.entry_count.to_le_bytes());
        meta_bytes.extend_from_slice(&meta.start_page.to_le_bytes());
        meta_bytes.extend_from_slice(&meta.end_page.to_le_bytes());
        meta_bytes.extend_from_slice(&meta.min_seq.to_le_bytes());
        meta_bytes.extend_from_slice(&meta.max_seq.to_le_bytes());
        meta_bytes.extend_from_slice(&(meta.min_key.len() as u32).to_le_bytes());
        meta_bytes.extend_from_slice(meta.min_key.as_bytes());
        meta_bytes.extend_from_slice(&(meta.max_key.len() as u32).to_le_bytes());
        meta_bytes.extend_from_slice(meta.max_key.as_bytes());

        let tuple = Tuple::new(vec![TupleValue::VarBytes(&meta_bytes)]);
        let cell_buffer = meta_page.allocate_cell(tuple.len())?;
        let mut cursor = std::io::Cursor::new(cell_buffer);
        tuple.write_to_stream(&mut cursor)?;

        Ok(())
    }
}

/// Reader for an SSTable.
pub struct SSTableReader {
    buffer_pool: Arc<BufferPool>,
    pub meta: SSTableMeta,
}

impl SSTableReader {
    /// Open an existing SSTable.
    pub fn open(buffer_pool: Arc<BufferPool>, file_id: u64) -> Result<Self, std::io::Error> {
        let meta = Self::read_metadata(&buffer_pool, file_id)?;
        Ok(Self { buffer_pool, meta })
    }

    fn read_metadata(buffer_pool: &BufferPool, file_id: u64) -> Result<SSTableMeta, std::io::Error> {
        let page_addr = PageAddr::new(file_id, 0);
        let page = Page::open(buffer_pool, page_addr)?;
        
        let cell = page.read_cell(0)?;
        let tuple = TupleOnDisk::new(cell);
        
        // The first field is VarBytes containing our metadata
        // We need to parse the null bitmap first, then the varint length
        let null_bitmap_len = 1; // 1 field = 1 byte
        let mut offset = null_bitmap_len;
        
        // Read varint length
        let (data_len, varint_size) = crate::tuple::varint::decode_varint(&tuple.data[offset..])?;
        offset += varint_size;
        
        let meta_bytes = &tuple.data[offset..offset + data_len as usize];
        let mut pos = 0;

        let id = u64::from_le_bytes(meta_bytes[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let entry_count = u64::from_le_bytes(meta_bytes[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let start_page = u64::from_le_bytes(meta_bytes[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let end_page = u64::from_le_bytes(meta_bytes[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let min_seq = u64::from_le_bytes(meta_bytes[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let max_seq = u64::from_le_bytes(meta_bytes[pos..pos + 8].try_into().unwrap());
        pos += 8;

        let min_key_len = u32::from_le_bytes(meta_bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let min_key = Key::from_slice(&meta_bytes[pos..pos + min_key_len]);
        pos += min_key_len;

        let max_key_len = u32::from_le_bytes(meta_bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4;
        let max_key = Key::from_slice(&meta_bytes[pos..pos + max_key_len]);

        Ok(SSTableMeta {
            id,
            entry_count,
            start_page,
            end_page,
            min_key,
            max_key,
            min_seq,
            max_seq,
        })
    }

    /// Check if a key might be in this SSTable (based on key range).
    pub fn might_contain(&self, key: &Key) -> bool {
        key >= &self.meta.min_key && key <= &self.meta.max_key
    }

    /// Get all entries for a key using binary search.
    pub fn get(&self, key: &Key) -> Result<Vec<Entry>, std::io::Error> {
        if !self.might_contain(key) {
            return Ok(vec![]);
        }

        let mut results = Vec::new();
        
        for page_id in self.meta.start_page..=self.meta.end_page {
            let page_addr = PageAddr::new(self.meta.id, page_id);
            let page = Page::open(&self.buffer_pool, page_addr)?;
            let num_cells = page.num_cells()?;
            
            if num_cells == 0 {
                continue;
            }

            // Check if key is in range for this page
            let first_entry = self.read_entry_from_page(&page, 0)?;
            let last_entry = self.read_entry_from_page(&page, num_cells - 1)?;
            
            if key < &first_entry.key {
                // Key is before this page, and since pages are sorted, 
                // it won't be in any subsequent page either
                break;
            }
            if key > &last_entry.key {
                // Key is after this page, check next page
                continue;
            }

            // Binary search to find first occurrence of key in this page
            let first_idx = self.binary_search_first(&page, key, num_cells)?;
            
            if let Some(idx) = first_idx {
                // Collect all entries with this key (they're consecutive)
                for cell_idx in idx..num_cells {
                    let entry = self.read_entry_from_page(&page, cell_idx)?;
                    if &entry.key == key {
                        results.push(entry);
                    } else {
                        // Keys are sorted, so we're done with this key
                        break;
                    }
                }
            }
            
            // If we found entries and the last one's key equals our key,
            // there might be more in the next page
            if let Some(last) = results.last() {
                if &last.key != key {
                    break; // No more entries for this key
                }
            }
        }

        Ok(results)
    }

    /// Binary search to find the first occurrence of a key in a page.
    /// Returns the index of the first entry with the given key, or None if not found.
    fn binary_search_first(&self, page: &Page, key: &Key, num_cells: usize) -> Result<Option<usize>, std::io::Error> {
        if num_cells == 0 {
            return Ok(None);
        }

        let mut left = 0;
        let mut right = num_cells;
        let mut result = None;

        while left < right {
            let mid = left + (right - left) / 2;
            let entry = self.read_entry_from_page(page, mid)?;

            match entry.key.cmp(key) {
                std::cmp::Ordering::Less => {
                    left = mid + 1;
                }
                std::cmp::Ordering::Equal => {
                    result = Some(mid);
                    right = mid; // Continue searching left for first occurrence
                }
                std::cmp::Ordering::Greater => {
                    right = mid;
                }
            }
        }

        Ok(result)
    }

    /// Iterate over all entries.
    pub fn iter(&self) -> SSTableIterator {
        SSTableIterator::new(self.buffer_pool.clone(), self.meta.clone())
    }

    fn read_entry_from_page(&self, page: &Page, cell_idx: usize) -> Result<Entry, std::io::Error> {
        let cell = page.read_cell(cell_idx)?;
        let tuple = TupleOnDisk::new(cell);
        
        // Parse: null_bitmap (1 byte) + varint length + data
        let null_bitmap_len = 1;
        let (data_len, varint_size) = crate::tuple::varint::decode_varint(&tuple.data[null_bitmap_len..])?;
        let entry_bytes = &tuple.data[null_bitmap_len + varint_size..null_bitmap_len + varint_size + data_len as usize];
        
        let (entry, _) = Entry::read_from(entry_bytes)?;
        Ok(entry)
    }
}

/// Iterator over SSTable entries.
pub struct SSTableIterator {
    buffer_pool: Arc<BufferPool>,
    meta: SSTableMeta,
    current_page: u64,
    current_cell: usize,
    cells_in_page: usize,
    initialized: bool,
    finished: bool,
}

impl SSTableIterator {
    fn new(buffer_pool: Arc<BufferPool>, meta: SSTableMeta) -> Self {
        Self {
            buffer_pool,
            current_page: meta.start_page,
            meta,
            current_cell: 0,
            cells_in_page: 0,
            initialized: false,
            finished: false,
        }
    }

    fn load_current_page(&mut self) -> Result<bool, std::io::Error> {
        if self.current_page > self.meta.end_page {
            self.finished = true;
            return Ok(false);
        }

        let page_addr = PageAddr::new(self.meta.id, self.current_page);
        let page = Page::open(&self.buffer_pool, page_addr)?;
        self.cells_in_page = page.num_cells()?;
        self.current_cell = 0;
        Ok(true)
    }
}

impl Iterator for SSTableIterator {
    type Item = Result<Entry, std::io::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }

        // Initialize on first call
        if !self.initialized {
            self.initialized = true;
            match self.load_current_page() {
                Ok(true) => {}
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }

        loop {
            // Try to read from current page
            if self.current_cell < self.cells_in_page {
                let page_addr = PageAddr::new(self.meta.id, self.current_page);
                match Page::open(&self.buffer_pool, page_addr) {
                    Ok(page) => {
                        let cell_idx = self.current_cell;
                        self.current_cell += 1;
                        
                        let cell = match page.read_cell(cell_idx) {
                            Ok(c) => c,
                            Err(e) => return Some(Err(e)),
                        };
                        
                        let tuple = TupleOnDisk::new(cell);
                        let null_bitmap_len = 1;
                        
                        let (data_len, varint_size) = match crate::tuple::varint::decode_varint(&tuple.data[null_bitmap_len..]) {
                            Ok(v) => v,
                            Err(e) => return Some(Err(e)),
                        };
                        
                        let entry_bytes = &tuple.data[null_bitmap_len + varint_size..null_bitmap_len + varint_size + data_len as usize];
                        
                        match Entry::read_from(entry_bytes) {
                            Ok((entry, _)) => return Some(Ok(entry)),
                            Err(e) => return Some(Err(e)),
                        }
                    }
                    Err(e) => return Some(Err(e)),
                }
            }

            // Move to next page
            self.current_page += 1;
            match self.load_current_page() {
                Ok(true) => continue,
                Ok(false) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::types::Value;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn get_temp_dir() -> String {
        let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        format!("/tmp/thordb_sstable_test_{}", since_epoch.as_nanos())
    }

    #[test]
    fn test_sstable_write_and_read() {
        let dir = get_temp_dir();
        let pool = Arc::new(BufferPool::new(dir.clone()).unwrap());
        let file_id = 1;

        // Write entries
        let entries = vec![
            Entry::put(Key::from("apple"), 1, Value::from("red")),
            Entry::put(Key::from("banana"), 2, Value::from("yellow")),
            Entry::put(Key::from("cherry"), 3, Value::from("red")),
        ];

        {
            let mut writer = SSTableWriter::new(&pool, file_id).unwrap();
            for entry in &entries {
                writer.write_entry(entry).unwrap();
            }
            let meta = writer.finish().unwrap();
            assert_eq!(meta.entry_count, 3);
        }

        // Read back
        {
            let reader = SSTableReader::open(pool.clone(), file_id).unwrap();
            assert_eq!(reader.meta.entry_count, 3);

            // Point lookup
            let results = reader.get(&Key::from("banana")).unwrap();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].value.as_ref().unwrap().as_bytes(), b"yellow");

            // Iterate
            let all: Vec<_> = reader.iter().collect();
            assert_eq!(all.len(), 3);
        }

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_sstable_duplicate_keys() {
        let dir = get_temp_dir();
        let pool = Arc::new(BufferPool::new(dir.clone()).unwrap());
        let file_id = 1;

        // Write entries with duplicate keys (different seq_nums)
        let entries = vec![
            Entry::put(Key::from("key"), 1, Value::from("v1")),
            Entry::put(Key::from("key"), 2, Value::from("v2")),
            Entry::put(Key::from("key"), 3, Value::from("v3")),
        ];

        {
            let mut writer = SSTableWriter::new(&pool, file_id).unwrap();
            for entry in &entries {
                writer.write_entry(entry).unwrap();
            }
            writer.finish().unwrap();
        }

        // Read back - should get all 3 entries
        {
            let reader = SSTableReader::open(pool.clone(), file_id).unwrap();
            let results = reader.get(&Key::from("key")).unwrap();
            assert_eq!(results.len(), 3);
        }

        let _ = std::fs::remove_dir_all(dir);
    }
}
