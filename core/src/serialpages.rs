use crate::{
    bufferpool::{BufferPool, PageAddr},
    page::{Page, PageMut, PageRead}, tuple::tuple::{Tuple, TupleOnDisk},
};

pub struct SerialWriter<'a> {
    buffer_pool: &'a BufferPool,
    page_writer: PageMut<'a>,
    page_address: PageAddr,
}

/// Reader for sequentially reading tuples across multiple pages.
/// Note: The streaming iterator pattern has lifetime limitations in Rust.
/// Consider using Page::read_cell directly for simpler access patterns.
#[allow(dead_code)]
pub struct SerialReader<'a> {
    buffer_pool: &'a BufferPool,
    page_reader: Page<'a>,
    page_address: PageAddr,
    end_page_address: PageAddr,
    num_cells_in_page: usize,
    current_cell_index: usize,
}

#[allow(dead_code)]
trait StreamingIterator<'a> {
    type Item;
    fn next(&'a mut self) -> Option<Self::Item>;
}

impl<'a> SerialWriter<'a> {
    pub fn new(buffer_pool: &'a BufferPool, page_address: PageAddr) -> Result<Self, std::io::Error> {
        let page_writer = PageMut::open(buffer_pool, page_address)?;
        Ok(Self {
            buffer_pool,
            page_writer,
            page_address,
        })
    }

    fn switch_page(&mut self, new_page: PageAddr) -> Result<(), std::io::Error> {
        self.page_writer = PageMut::open(self.buffer_pool, new_page)?;
        self.page_address = new_page;
        Ok(())
    }

    pub fn append_tuple(&mut self, tuple: &Tuple) -> Result<(), std::io::Error> {
        if !self.page_writer.has_space_for_cell(tuple.len())? {
            self.switch_page(self.page_address.next_page())?;
        }
        let tuple_len = tuple.len();
        let cell_buffer = self.page_writer.allocate_cell(tuple_len)?;
        let mut cursor = std::io::Cursor::new(cell_buffer);
        let bytes_written = tuple.write_to_stream(&mut cursor)?;
        assert_eq!(bytes_written, tuple_len);
        Ok(())
    }
}

impl<'a> SerialReader<'a> {
    pub fn new(buffer_pool: &'a BufferPool, start_page_address: PageAddr, end_page_address: PageAddr) -> Result<Self, std::io::Error> {
        let page_reader = Page::open(buffer_pool, start_page_address)?;
        let num_cells_in_page = page_reader.num_cells()?;
        Ok(Self {
            buffer_pool,
            page_reader,
            page_address: start_page_address,
            end_page_address,
            num_cells_in_page,
            current_cell_index: 0,
        })
    }

    #[allow(dead_code)]
    fn switch_page(&mut self, new_page: PageAddr) -> Result<(), std::io::Error> {
        self.page_reader = Page::open(self.buffer_pool, new_page)?;
        self.page_address = new_page;
        self.num_cells_in_page = self.page_reader.num_cells()?;
        self.current_cell_index = 0;
        Ok(())
    }
}

impl<'a> StreamingIterator<'a> for SerialReader<'a> {

    type Item = Result<TupleOnDisk<'a>, std::io::Error>;

    fn next(&'a mut self) -> Option<Self::Item> {
        if self.current_cell_index >= self.num_cells_in_page {
            if self.page_address == self.end_page_address {
                return None;
            }
            if let Err(e) = self.switch_page(self.page_address.next_page()) {
                return Some(Err(e));
            }
        }
        match self.page_reader.read_cell(self.current_cell_index) {
            Ok(cell_data) => {
                self.current_cell_index += 1;
                Some(Ok(TupleOnDisk::new(cell_data)))
            }
            Err(e) => Some(Err(e)),
        }
    }
}







#[cfg(test)]
mod tests {
    use super::*;
    use crate::tuple::types::{TupleDescriptor, TupleFieldDescriptor, TupleFieldType, TupleValue};
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn get_temp_dir() -> String {
        let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        format!("/tmp/thordb_serial_test_{}", since_epoch.as_nanos())
    }

    #[test]
    fn test_serial_tuple_write_and_read() {
        let dir = get_temp_dir();
        let pool = Arc::new(BufferPool::new(dir.clone()).unwrap());
        let page_addr = PageAddr::new(1, 0);

        // Create descriptor
        let mut descriptor = TupleDescriptor::new();
        descriptor.add_field(TupleFieldDescriptor::new("id".to_string(), TupleFieldType::Int64));
        descriptor.add_field(TupleFieldDescriptor::new("name".to_string(), TupleFieldType::VarBytes));
        descriptor.add_field(TupleFieldDescriptor::new("age".to_string(), TupleFieldType::Int32));

        // Write tuples
        {
            let mut writer = SerialWriter::new(&pool, page_addr).unwrap();

            let tuple1 = Tuple::new(vec![
                TupleValue::Int64(1),
                TupleValue::VarBytes(b"Alice"),
                TupleValue::Int32(30),
            ]);
            writer.append_tuple(&tuple1).unwrap();

            let tuple2 = Tuple::new(vec![
                TupleValue::Int64(2),
                TupleValue::VarBytes(b"Bob"),
                TupleValue::Null,
            ]);
            writer.append_tuple(&tuple2).unwrap();
        }

        // Read tuples back - use page directly to avoid streaming iterator lifetime issues
        {
            let page = Page::open(&pool, page_addr).unwrap();
            assert_eq!(page.num_cells().unwrap(), 2);

            // Read first tuple
            let cell1 = page.read_cell(0).unwrap();
            let tuple1 = TupleOnDisk::new(cell1);
            assert_eq!(tuple1.read_field(&descriptor, 0).unwrap(), TupleValue::Int64(1));
            assert_eq!(tuple1.read_field(&descriptor, 1).unwrap(), TupleValue::VarBytes(b"Alice"));
            assert_eq!(tuple1.read_field(&descriptor, 2).unwrap(), TupleValue::Int32(30));

            // Read second tuple
            let cell2 = page.read_cell(1).unwrap();
            let tuple2 = TupleOnDisk::new(cell2);
            assert_eq!(tuple2.read_field(&descriptor, 0).unwrap(), TupleValue::Int64(2));
            assert_eq!(tuple2.read_field(&descriptor, 1).unwrap(), TupleValue::VarBytes(b"Bob"));
            assert_eq!(tuple2.read_field(&descriptor, 2).unwrap(), TupleValue::Null);
        }

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_serial_multiple_tuples() {
        let dir = get_temp_dir();
        let pool = Arc::new(BufferPool::new(dir.clone()).unwrap());
        let page_addr = PageAddr::new(1, 0);

        // Simple descriptor with just integers
        let mut descriptor = TupleDescriptor::new();
        descriptor.add_field(TupleFieldDescriptor::new("value".to_string(), TupleFieldType::Int32));

        let test_values: Vec<i32> = vec![100, 200, 300, 400, 500];

        // Write tuples
        {
            let mut writer = SerialWriter::new(&pool, page_addr).unwrap();
            for val in &test_values {
                let tuple = Tuple::new(vec![TupleValue::Int32(*val)]);
                writer.append_tuple(&tuple).unwrap();
            }
        }

        // Read tuples back using Page directly
        {
            let page = Page::open(&pool, page_addr).unwrap();
            assert_eq!(page.num_cells().unwrap(), test_values.len());
            
            for (i, expected_val) in test_values.iter().enumerate() {
                let cell = page.read_cell(i).unwrap();
                let tuple = TupleOnDisk::new(cell);
                assert_eq!(
                    tuple.read_field(&descriptor, 0).unwrap(),
                    TupleValue::Int32(*expected_val)
                );
            }
        }

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn test_tuple_with_nulls() {
        let dir = get_temp_dir();
        let pool = Arc::new(BufferPool::new(dir.clone()).unwrap());
        let page_addr = PageAddr::new(1, 0);

        let mut descriptor = TupleDescriptor::new();
        descriptor.add_field(TupleFieldDescriptor::new("a".to_string(), TupleFieldType::Int32));
        descriptor.add_field(TupleFieldDescriptor::new("b".to_string(), TupleFieldType::Int32));
        descriptor.add_field(TupleFieldDescriptor::new("c".to_string(), TupleFieldType::Int32));

        // Write tuple with null in middle
        {
            let mut writer = SerialWriter::new(&pool, page_addr).unwrap();
            let tuple = Tuple::new(vec![
                TupleValue::Int32(10),
                TupleValue::Null,
                TupleValue::Int32(30),
            ]);
            writer.append_tuple(&tuple).unwrap();
        }

        // Read and verify using Page directly
        {
            let page = Page::open(&pool, page_addr).unwrap();
            let cell = page.read_cell(0).unwrap();
            let tuple = TupleOnDisk::new(cell);
            assert_eq!(tuple.read_field(&descriptor, 0).unwrap(), TupleValue::Int32(10));
            assert_eq!(tuple.read_field(&descriptor, 1).unwrap(), TupleValue::Null);
            assert_eq!(tuple.read_field(&descriptor, 2).unwrap(), TupleValue::Int32(30));
        }

        let _ = std::fs::remove_dir_all(dir);
    }
}
