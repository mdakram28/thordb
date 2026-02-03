use parking_lot::{RwLockReadGuard, RwLockWriteGuard};

use crate::{
    bufferpool::{BufferPool, BufferSlot, PageAddr}, constants::PAGE_SIZE,
};

/**
 * Page format:
 * - free_start: u16
 * - free_end: u16
 * - cell_pointers:
 *   - cell_start_offset: u16
 *   - cell_size: u16
 * - free space
 * - cells: [u8;]
 */

const FREE_START_OFFSET_OFFSET: usize = 0;
const FREE_END_OFFSET_OFFSET: usize = 2;
const CELL_POINTERS_OFFSET: usize = 4;
const CELL_POINTER_SIZE: usize = 4;

pub struct Page<'a> {
    slot: RwLockReadGuard<'a, Box<BufferSlot>>,
}

pub struct PageMut<'a> {
    slot: RwLockWriteGuard<'a, Box<BufferSlot>>,
}

pub trait PageRead {
    fn page_data(&self) -> &[u8];

    fn read_u16(&self, offset: usize) -> Result<u16, std::io::Error> {
        let bytes = &self.page_data()[offset..offset + 2];
        Ok(u16::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn read_cell_pointer(&self, entry_index: usize) -> Result<(usize, usize), std::io::Error> {
        let cell_pointer_offset: usize = CELL_POINTERS_OFFSET + entry_index * CELL_POINTER_SIZE;
        let cell_start_offset: usize = self.read_u16(cell_pointer_offset)? as usize;
        let cell_end_offset: usize = cell_start_offset + self.read_u16(cell_pointer_offset + 2)? as usize;
        assert!(cell_start_offset < cell_end_offset && cell_end_offset <= PAGE_SIZE);
        Ok((cell_start_offset, cell_end_offset))
    }

    fn read_cell(&self, entry_index: usize) -> Result<&[u8], std::io::Error> {
        let (cell_start_offset, cell_end_offset) = self.read_cell_pointer(entry_index)?;
        Ok(&self.page_data()[cell_start_offset..cell_end_offset])
    }

    fn num_cells(&self) -> Result<usize, std::io::Error> {
        let free_start_offset: usize = self.read_u16(FREE_START_OFFSET_OFFSET)? as usize;
        Ok((free_start_offset - CELL_POINTERS_OFFSET) / CELL_POINTER_SIZE)
    }
}

impl<'a> PageRead for Page<'a> {
    fn page_data(&self) -> &[u8] {
        &self.slot.page_data
    }
}

impl<'a> PageRead for PageMut<'a> {
    fn page_data(&self) -> &[u8] {
        &self.slot.page_data
    }
}

impl<'a> Page<'a> {
    pub fn open(buffer_pool: &'a BufferPool, page_address: PageAddr) -> Result<Self, std::io::Error> {
        let slot = buffer_pool.pin_read(page_address)?;
        Ok(Self { slot })
    }
}

impl<'a> PageMut<'a> {
    pub fn open(buffer_pool: &'a BufferPool, page_address: PageAddr) -> Result<Self, std::io::Error> {
        let slot = buffer_pool.pin_write(page_address)?;
        let mut page = Self { slot };
        // Initialize page if it's empty (new page)
        if page.read_u16(FREE_START_OFFSET_OFFSET)? == 0 && page.read_u16(FREE_END_OFFSET_OFFSET)? == 0 {
            page.init()?;
        }
        Ok(page)
    }

    /// Initialize an empty page with proper free space pointers
    fn init(&mut self) -> Result<(), std::io::Error> {
        // free_start points to where the next cell pointer will be written
        self.write_u16(FREE_START_OFFSET_OFFSET, CELL_POINTERS_OFFSET as u16)?;
        // free_end points to where the next cell data will be written (end of page)
        self.write_u16(FREE_END_OFFSET_OFFSET, PAGE_SIZE as u16)?;
        Ok(())
    }

    fn write_u16(&mut self, offset: usize, value: u16) -> Result<(), std::io::Error> {
        let bytes = value.to_le_bytes();
        self.slot.page_data[offset..offset + 2].copy_from_slice(&bytes);
        Ok(())
    }

    pub fn has_space_for_cell(&self, len: usize) -> Result<bool, std::io::Error> {
        let free_start_offset: usize = self.read_u16(FREE_START_OFFSET_OFFSET)? as usize;
        let free_end_offset: usize = self.read_u16(FREE_END_OFFSET_OFFSET)? as usize;
        Ok(free_end_offset - free_start_offset >= len + CELL_POINTER_SIZE)
    }

    pub fn allocate_cell(&mut self, cell_len: usize) -> Result<&mut [u8], std::io::Error> {
        assert!(self.has_space_for_cell(cell_len)?);
        let free_start_offset: usize = self.read_u16(FREE_START_OFFSET_OFFSET)? as usize;
        let free_end_offset: usize = self.read_u16(FREE_END_OFFSET_OFFSET)? as usize;

        // Write cell pointer
        self.write_u16(free_start_offset, (free_end_offset - cell_len) as u16)?;
        self.write_u16(free_start_offset + 2, cell_len as u16)?;

        // Update free space
        self.write_u16(FREE_START_OFFSET_OFFSET, (free_start_offset + CELL_POINTER_SIZE) as u16)?;
        self.write_u16(FREE_END_OFFSET_OFFSET, (free_end_offset - cell_len) as u16)?;

        // Return cell data
        Ok(&mut self.slot.page_data[free_end_offset - cell_len..free_end_offset])
    }
}
