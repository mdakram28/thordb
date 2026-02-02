use parking_lot::{RwLockReadGuard, RwLockWriteGuard};

use crate::{
    bufferpool::{BufferPool, BufferSlot, PageAddr},
    constants::PAGE_SIZE,
};

pub struct Page<'a> {
    slot: RwLockReadGuard<'a, Box<BufferSlot>>,
}

pub struct PageMut<'a> {
    slot: RwLockWriteGuard<'a, Box<BufferSlot>>,
}

pub trait BinarySerializable {
    fn write_to(&self, buffer: &mut [u8]);
    fn read_from(buffer: &[u8]) -> Result<Self, std::io::Error>
    where
        Self: Sized;
}

impl<'a> Page<'a> {
    pub fn open(buffer_pool: &'a BufferPool, page_address: PageAddr) -> Result<Self, std::io::Error> {
        let slot = buffer_pool.pin_read(page_address)?;
        Ok(Self { slot })
    }

    pub fn read<T: BinarySerializable>(&self, offset: u64) -> Result<T, std::io::Error> {
        let bytes = &self.slot.page_data[offset as usize..];
        T::read_from(bytes)
    }

    pub fn read_bytes(&self, offset: u64, len: usize) -> &[u8] {
        &self.slot.page_data[offset as usize..(offset as usize + len)]
    }
}

impl<'a> PageMut<'a> {
    pub fn open(buffer_pool: &'a BufferPool, page_address: PageAddr) -> Result<Self, std::io::Error> {
        let slot = buffer_pool.pin_write(page_address)?;
        Ok(Self { slot })
    }

    pub fn write<T: BinarySerializable>(&mut self, offset: u64, data: &T) -> Result<(), std::io::Error> {
        data.write_to(&mut self.slot.page_data[offset as usize..]);
        Ok(())
    }

    pub fn read<T: BinarySerializable>(&self, offset: u64) -> Result<T, std::io::Error> {
        let bytes = &self.slot.page_data[offset as usize..];
        T::read_from(bytes)
    }

    pub fn write_bytes(&mut self, offset: u64, data: &[u8]) -> Result<(), std::io::Error> {
        self.slot.page_data[offset as usize..(offset as usize + data.len())].copy_from_slice(data);
        Ok(())
    }
}
