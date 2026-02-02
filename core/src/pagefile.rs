use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

use parking_lot::Mutex;

use crate::constants::PAGE_SIZE;

pub struct PageFile {
    file_handle: Mutex<File>,
}

impl PageFile {
    pub fn new(file: &PathBuf) -> Result<Self, std::io::Error> {
        let file_handle = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file)?;
        Ok(Self {
            file_handle: Mutex::new(file_handle),
        })
    }

    pub fn read_page(&self, page_id: u64, buffer: &mut [u8], create: bool) -> Result<(), std::io::Error> {
        let offset = page_id * PAGE_SIZE;
        tracing::info!("Reading page {}", page_id);
        self.file_handle.lock().read_at(buffer, offset).map(|bytes| {
            if bytes as u64 != PAGE_SIZE {
                if create {
                    buffer.fill(0);
                } else {
                    panic!("Read partial page");
                }
            }
        })
    }

    pub fn write_page(&self, page_id: u64, buffer: &[u8]) -> Result<(), std::io::Error> {
        assert!(buffer.len() == PAGE_SIZE as usize);
        tracing::info!("Writing page {}", page_id);

        let offset = page_id * PAGE_SIZE;
        self.file_handle.lock().write_at(buffer, offset).map(|bytes| {
            assert_eq!(bytes as u64, PAGE_SIZE, "Wrote partial page");
        })
    }
}
