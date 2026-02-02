use dashmap::{DashMap, mapref::one::Ref};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{constants::*, pagefile::PageFile};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug)]
pub struct PageAddr {
    file_id: u64,
    page_id: u64,
}

pub struct BufferSlot {
    pub page_address: PageAddr,
    pub page_data: [u8; PAGE_SIZE as usize],
    is_dirty: bool,
}

pub struct BufferPool {
    slots: [RwLock<Box<BufferSlot>>; BUFFER_POOL_SIZE],
    slots_touched: [AtomicBool; BUFFER_POOL_SIZE],
    page_to_slot: DashMap<PageAddr, usize>,
    next_slot: AtomicUsize,
    page_files_map: DashMap<u64, PageFile>,
    page_files_dir: PathBuf,
}

impl PageAddr {
    pub fn new(file_id: u64, page_id: u64) -> Self {
        Self { file_id, page_id }
    }

    pub fn next_page(&self) -> Self {
        Self {
            file_id: self.file_id,
            page_id: self.page_id + 1,
        }
    }
}

impl BufferSlot {
    fn new() -> Self {
        Self {
            page_address: PageAddr { file_id: 0, page_id: 0 },
            page_data: [0; PAGE_SIZE as usize],
            is_dirty: false,
        }
    }

    fn load_page(&mut self, page_address: &PageAddr, page_file: &PageFile, create: bool) -> Result<(), std::io::Error> {
        self.page_address = *page_address;
        page_file.read_page(page_address.page_id, &mut self.page_data, create)?;
        self.is_dirty = false;
        Ok(())
    }

    fn write_page(&mut self, page_file: &PageFile) -> Result<(), std::io::Error> {
        page_file.write_page(self.page_address.page_id, &mut self.page_data)?;
        self.is_dirty = false;
        Ok(())
    }
}

impl<'a> BufferPool {
    pub fn new(page_files_dir: String) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(&page_files_dir)?;
        Ok(Self {
            slots: std::array::from_fn(|_| RwLock::new(Box::new(BufferSlot::new()))),
            slots_touched: std::array::from_fn(|_| AtomicBool::new(false)),
            page_to_slot: DashMap::new(),
            next_slot: AtomicUsize::new(0),
            page_files_map: DashMap::new(),
            page_files_dir: PathBuf::from(page_files_dir),
        })
    }

    pub fn pin_read(&self, page_address: PageAddr) -> Result<RwLockReadGuard<'_, Box<BufferSlot>>, std::io::Error> {
        loop {
            // Fast path: check if page is already in the map
            if let Some(map_guard) = self.page_to_slot.get(&page_address) {
                let slot_index = *map_guard;
                drop(map_guard);

                let slot = self.slots[slot_index].read();
                if slot.page_address != page_address {
                    println!("Page {:?} found in buffer pool but modified before lock", page_address);
                    continue;
                }
                self.slots_touched[slot_index].store(true, Ordering::Relaxed);
                return Ok(slot);
            }

            // Slow path: Allocate a new slot *first* (without holding map lock)
            let slot_guard = self.allocate_slot(&page_address, false)?;
            return Ok(RwLockWriteGuard::downgrade(slot_guard));
        }
    }

    pub fn pin_write(&self, page_address: PageAddr) -> Result<RwLockWriteGuard<'_, Box<BufferSlot>>, std::io::Error> {
        loop {
            // Fast path: check if page is already in the map
            if let Some(map_guard) = self.page_to_slot.get(&page_address) {
                let slot_index = *map_guard;
                drop(map_guard);

                let mut slot = self.slots[slot_index].write();
                if slot.page_address != page_address {
                    println!("Page {:?} found in buffer pool but modified before lock", page_address);
                    continue;
                }
                self.slots_touched[slot_index].store(true, Ordering::Relaxed);
                slot.is_dirty = true;
                return Ok(slot);
            }

            // Slow path: Allocate a new slot *first* (without holding map lock)
            let mut slot_guard = self.allocate_slot(&page_address, true)?;
            slot_guard.is_dirty = true;
            return Ok(slot_guard);
        }
    }

    fn allocate_slot(
        &self,
        page_address: &PageAddr,
        create_if_not_exists: bool,
    ) -> Result<RwLockWriteGuard<'_, Box<BufferSlot>>, std::io::Error> {
        for _ in 0..BUFFER_POOL_SIZE * 2 {
            let slot_index = self.next_slot.fetch_add(1, Ordering::Relaxed) % BUFFER_POOL_SIZE;
            if self.slots_touched[slot_index].load(Ordering::Acquire) {
                self.slots_touched[slot_index].store(false, Ordering::Relaxed);
                continue;
            }
            if let Some(mut slot) = self.slots[slot_index].try_write() {
                if slot.is_dirty {
                    let page_file = self.get_page_file(slot.page_address.file_id)?;
                    slot.write_page(&page_file)?;
                }
                // Clear old slot
                self.page_to_slot.remove(&slot.page_address);
                self.slots_touched[slot_index].store(true, Ordering::Relaxed);

                // Fast path: check if page file is already in the map
                let page_file = self.get_page_file(page_address.file_id)?;
                slot.load_page(page_address, &page_file, create_if_not_exists)?;
                self.page_to_slot.insert(*page_address, slot_index);
                return Ok(slot);
            }
        }
        Err(std::io::Error::new(std::io::ErrorKind::Other, "Buffer pool is full"))
    }

    fn get_page_file(&self, file_id: u64) -> Result<Ref<'_, u64, PageFile>, std::io::Error> {
        if let Some(page_file) = self.page_files_map.get(&file_id) {
            return Ok(page_file);
        }

        match self.page_files_map.entry(file_id) {
            dashmap::mapref::entry::Entry::Occupied(occupied) => {
                let page_file = occupied.into_ref().downgrade();
                return Ok(page_file);
            }
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                let file_name = format!("{:0PAGE_FILE_NUM_DIGITS$}.pagefile", file_id);
                let file_path = self.page_files_dir.join(file_name);
                let page_file = vacant.insert(PageFile::new(&file_path)?).downgrade();
                return Ok(page_file);
            }
        }
    }

    pub fn flush(&self) -> Result<(), std::io::Error> {
        for slot in self.slots.iter() {
            if let Some(mut slot) = slot.try_write() {
                if slot.is_dirty {
                    let page_file = self.get_page_file(slot.page_address.file_id)?;
                    slot.write_page(&page_file)?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn get_temp_dir() -> String {
        let start = SystemTime::now();
        let since_the_epoch = start.duration_since(UNIX_EPOCH).expect("Time went backwards");
        let dir_name = format!("/tmp/thordb_test_{}", since_the_epoch.as_nanos());
        dir_name
    }

    #[test]
    fn test_buffer_pool_simple_read_write() {
        let dir = get_temp_dir();
        let pool = BufferPool::new(dir.clone()).unwrap();
        let page_addr = PageAddr { file_id: 1, page_id: 0 };

        // Write
        {
            let mut slot = pool.pin_write(page_addr).unwrap();
            slot.page_data[0] = 42;
        } // Drop write lock

        // Read
        {
            let slot = pool.pin_read(page_addr).unwrap();
            assert_eq!(slot.page_data[0], 42);
        }

        // Clean up
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_buffer_pool_eviction() {
        let dir = get_temp_dir();
        let pool = BufferPool::new(dir.clone()).unwrap();

        // Fill pool + trigger eviction (Size is 50)
        for i in 0..BUFFER_POOL_SIZE + 10 {
            let page_addr = PageAddr {
                file_id: 1,
                page_id: i as u64,
            };
            let mut slot = pool.pin_write(page_addr).unwrap();
            slot.page_data[0] = (i % 255) as u8;
            // Drop lock immediately
        }

        // Verify size limit behavior (it's hard to assert exact size as eviction might lag or be exact,
        // but DashMap size should be close to limit. Actually, allocate_slot removes old entry *before* inserting new.
        // So map size should never exceed BUFFER_POOL_SIZE).
        assert!(pool.page_to_slot.len() <= BUFFER_POOL_SIZE);

        // Verify persistence of evicted page (Page 0 likely evicted as it was first)
        // With Clock algorithm, Page 0 was touched, so it might have survived one pass?
        // But we wrote 60 pages into 50 slots. At least 10 must be evicted.
        // Let's check Page 0. Even if evicted, pin_read should reload it from disk.

        let page0 = PageAddr { file_id: 1, page_id: 0 };
        {
            let slot = pool.pin_read(page0).unwrap();
            assert_eq!(slot.page_data[0], 0);
        }

        // Clean up
        let _ = std::fs::remove_dir_all(dir);
    }
}
