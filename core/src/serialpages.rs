use crate::{
    bufferpool::{BufferPool, PageAddr},
    constants::PAGE_SIZE,
    page::{Page, PageMut},
    tuple::{Tuple, TupleDescriptor},
};

/**
 * Serial pages are pages that are written in a serial manner.
 * Page format:
 * - end_offset: u32
 * - data: [u8; PAGE_SIZE - 4]
 */

const DATA_START_OFFSET: u64 = size_of::<u32>() as u64;

pub struct SerialWriter<'a> {
    buffer_pool: &'a BufferPool,
    page_offset: u32,
    page_writer: PageMut<'a>,
    page_address: PageAddr,
}

pub struct SerialReader<'a> {
    buffer_pool: &'a BufferPool,
    page_offset: u32,
    page_reader: Page<'a>,
    page_address: PageAddr,
}

impl<'a> SerialWriter<'a> {
    pub fn new(buffer_pool: &'a BufferPool, page_address: PageAddr) -> Result<Self, std::io::Error> {
        let page_writer = PageMut::open(buffer_pool, page_address)?;
        let end_offset = page_writer.read::<u32>(0)?;
        let page_offset = if end_offset == 0 {
            DATA_START_OFFSET as u32
        } else {
            end_offset
        };

        let mut result = Self {
            buffer_pool,
            page_offset,
            page_writer,
            page_address,
        };

        // Initialize header if new page
        if end_offset == 0 {
            result.page_writer.write::<u32>(0, &(DATA_START_OFFSET as u32))?;
        }

        Ok(result)
    }

    pub fn switch_page(&mut self, new_page: PageAddr) -> Result<(), std::io::Error> {
        self.page_writer = PageMut::open(self.buffer_pool, new_page)?;
        self.page_address = new_page;
        self.page_offset = DATA_START_OFFSET as u32;

        let end_offset = self.page_writer.read::<u32>(0)?;
        if end_offset == 0 {
            self.page_writer.write::<u32>(0, &(DATA_START_OFFSET as u32))?;
        }
        if end_offset != 0 {
            self.page_offset = end_offset;
        }
        Ok(())
    }

    pub fn write(&mut self, data: &[u8]) -> Result<(), std::io::Error> {
        if data.len() as u64 > PAGE_SIZE - DATA_START_OFFSET {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "data too large"));
        }

        if self.page_offset as u64 + data.len() as u64 > PAGE_SIZE as u64 {
            self.switch_page(self.page_address.next_page())?;
        }
        self.page_writer.write_bytes(self.page_offset as u64, data)?;
        self.page_offset += data.len() as u32;
        self.page_writer.write::<u32>(0, &self.page_offset)?;
        Ok(())
    }

    /// Appends a tuple to the serial page, serialized according to the descriptor
    pub fn append_tuple(&mut self, tuple: &Tuple, descriptor: &TupleDescriptor) -> Result<(), std::io::Error> {
        let serialized = tuple.serialize(descriptor)?;
        // Write length prefix so reader knows how much to read
        let len = serialized.len() as u32;
        let mut data = len.to_le_bytes().to_vec();
        data.extend_from_slice(&serialized);
        self.write(&data)
    }
}

impl<'a> SerialReader<'a> {
    pub fn new(buffer_pool: &'a BufferPool, page_address: PageAddr) -> Result<Self, std::io::Error> {
        Ok(Self {
            buffer_pool,
            page_offset: DATA_START_OFFSET as u32,
            page_reader: Page::open(buffer_pool, page_address)?,
            page_address,
        })
    }

    pub fn switch_page(&mut self, new_page: PageAddr) -> Result<(), std::io::Error> {
        self.page_reader = Page::open(self.buffer_pool, new_page)?;
        self.page_address = new_page;
        self.page_offset = DATA_START_OFFSET as u32;
        Ok(())
    }

    pub fn read(&mut self, len: usize) -> Result<&[u8], std::io::Error> {
        let end_offset = self.page_reader.read::<u32>(0)?;

        if self.page_offset as u64 + len as u64 > end_offset as u64 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "not enough data",
            ));
        }

        let data = self.page_reader.read_bytes(self.page_offset as u64, len);
        self.page_offset += len as u32;
        Ok(data)
    }

    pub fn has_more(&self) -> Result<bool, std::io::Error> {
        let end_offset = self.page_reader.read::<u32>(0)?;
        Ok(self.page_offset < end_offset)
    }

    /// Reads a tuple from the serial page, deserialized according to the descriptor
    pub fn read_tuple(&mut self, descriptor: &TupleDescriptor) -> Result<Tuple, std::io::Error> {
        // Read length prefix
        let len_bytes = self.read(4)?;
        let len = u32::from_le_bytes(len_bytes.try_into().unwrap()) as usize;

        // Read tuple data
        let tuple_bytes = self.read(len)?;
        let (tuple, _) = Tuple::deserialize(tuple_bytes, descriptor)?;
        Ok(tuple)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn get_temp_dir() -> String {
        let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        format!("/tmp/thordb_serial_test_{}", since_epoch.as_nanos())
    }

    #[test]
    fn test_serial_write_and_read() {
        let dir = get_temp_dir();
        let pool = Arc::new(BufferPool::new(dir.clone()).unwrap());
        let page_addr = PageAddr::new(1, 0);

        // Write data
        {
            let mut writer = SerialWriter::new(&pool, page_addr).unwrap();
            writer.write(b"hello").unwrap();
            writer.write(b"world").unwrap();
        }

        // Read data back
        {
            let mut reader = SerialReader::new(&pool, page_addr).unwrap();

            let first = reader.read(5).unwrap();
            assert_eq!(first, b"hello");

            let second = reader.read(5).unwrap();
            assert_eq!(second, b"world");

            assert!(!reader.has_more().unwrap());
        }

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_serial_multiple_writes_and_reads() {
        let dir = get_temp_dir();
        let pool = Arc::new(BufferPool::new(dir.clone()).unwrap());
        let page_addr = PageAddr::new(1, 0);

        let test_data: Vec<&[u8]> = vec![b"first", b"second", b"third"];

        // Write
        {
            let mut writer = SerialWriter::new(&pool, page_addr).unwrap();
            for data in &test_data {
                writer.write(data).unwrap();
            }
        }

        // Read
        {
            let mut reader = SerialReader::new(&pool, page_addr).unwrap();
            for expected in &test_data {
                let actual = reader.read(expected.len()).unwrap();
                assert_eq!(actual, *expected);
            }
            assert!(!reader.has_more().unwrap());
        }

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_serial_tuple_write_and_read() {
        use crate::tuple::{ColumnType, TupleValue};

        let dir = get_temp_dir();
        let pool = Arc::new(BufferPool::new(dir.clone()).unwrap());
        let page_addr = PageAddr::new(1, 0);

        // Create descriptor
        let mut descriptor = TupleDescriptor::new();
        descriptor
            .add_column("id", ColumnType::Int64, false)
            .add_column("name", ColumnType::VarString, false)
            .add_column("age", ColumnType::Int32, true);

        // Write tuples
        {
            let mut writer = SerialWriter::new(&pool, page_addr).unwrap();

            let tuple1 = Tuple::with_values(vec![
                TupleValue::Int64(1),
                TupleValue::String("Alice".to_string()),
                TupleValue::Int32(30),
            ]);
            writer.append_tuple(&tuple1, &descriptor).unwrap();

            let tuple2 = Tuple::with_values(vec![
                TupleValue::Int64(2),
                TupleValue::String("Bob".to_string()),
                TupleValue::Null,
            ]);
            writer.append_tuple(&tuple2, &descriptor).unwrap();
        }

        // Read tuples back
        {
            let mut reader = SerialReader::new(&pool, page_addr).unwrap();

            let read_tuple1 = reader.read_tuple(&descriptor).unwrap();
            assert_eq!(read_tuple1.values[0], TupleValue::Int64(1));
            assert_eq!(read_tuple1.values[1], TupleValue::String("Alice".to_string()));
            assert_eq!(read_tuple1.values[2], TupleValue::Int32(30));

            let read_tuple2 = reader.read_tuple(&descriptor).unwrap();
            assert_eq!(read_tuple2.values[0], TupleValue::Int64(2));
            assert_eq!(read_tuple2.values[1], TupleValue::String("Bob".to_string()));
            assert_eq!(read_tuple2.values[2], TupleValue::Null);

            assert!(!reader.has_more().unwrap());
        }

        let _ = std::fs::remove_dir_all(dir);
    }
}
