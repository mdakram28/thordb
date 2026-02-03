// Public API
pub mod bufferpool;
pub mod lsm;
pub mod page;
pub mod serialpages;
pub mod tuple;

// Internal modules
pub(crate) mod constants;
pub(crate) mod pagefile;

use std::{sync::Arc, thread};

use crate::constants::BG_FLUSH_INTERVAL_MS;

pub struct ThorDB {
    #[allow(dead_code)] // Will be used for database operations
    buffer_pool: Arc<bufferpool::BufferPool>,
    bg_flush_thread: Option<std::thread::JoinHandle<()>>,
}

impl ThorDB {
    pub fn new(data_dir: &str) -> Result<Self, std::io::Error> {
        std::fs::create_dir_all(data_dir)?;
        let page_files_dir = format!("{}/pagestore", data_dir);
        let buffer_pool = Arc::new(bufferpool::BufferPool::new(page_files_dir)?);

        let buffer_pool_clone = Arc::clone(&buffer_pool);
        let bg_flush_thread = Some(std::thread::spawn(move || {
            loop {
                thread::sleep(std::time::Duration::from_millis(BG_FLUSH_INTERVAL_MS));
                println!("Flushing buffer pool");
                buffer_pool_clone.flush().unwrap();
            }
        }));

        Ok(Self {
            buffer_pool,
            bg_flush_thread,
        })
    }

    pub fn close(self) {
        self.bg_flush_thread.unwrap().join().unwrap();
    }
}
