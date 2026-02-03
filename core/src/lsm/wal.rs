//! Write-Ahead Log (WAL) for durability.
//!
//! All writes are logged to the WAL before being applied to the memtable.
//! On crash recovery, the WAL is replayed to restore the memtable state.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use super::types::{Entry, Key, SeqNum, Value};

/// Write-ahead log for durability.
pub struct Wal {
    writer: BufWriter<File>,
    path: String,
}

/// WAL entry type markers.
const WAL_PUT: u8 = 1;
const WAL_DELETE: u8 = 2;

impl Wal {
    /// Create or open a WAL file.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        
        Ok(Self {
            writer: BufWriter::new(file),
            path: path_str,
        })
    }

    /// Log a put operation.
    pub fn log_put(&mut self, key: &Key, value: &Value, seq_num: SeqNum) -> Result<(), std::io::Error> {
        // Format: type (1) + seq_num (8) + key_len (4) + key + value_len (4) + value
        self.writer.write_all(&[WAL_PUT])?;
        self.writer.write_all(&seq_num.to_le_bytes())?;
        self.writer.write_all(&(key.len() as u32).to_le_bytes())?;
        self.writer.write_all(key.as_bytes())?;
        self.writer.write_all(&(value.len() as u32).to_le_bytes())?;
        self.writer.write_all(value.as_bytes())?;
        self.writer.flush()?;
        Ok(())
    }

    /// Log a delete operation.
    pub fn log_delete(&mut self, key: &Key, seq_num: SeqNum) -> Result<(), std::io::Error> {
        // Format: type (1) + seq_num (8) + key_len (4) + key
        self.writer.write_all(&[WAL_DELETE])?;
        self.writer.write_all(&seq_num.to_le_bytes())?;
        self.writer.write_all(&(key.len() as u32).to_le_bytes())?;
        self.writer.write_all(key.as_bytes())?;
        self.writer.flush()?;
        Ok(())
    }

    /// Sync the WAL to disk.
    #[allow(dead_code)]
    pub fn sync(&mut self) -> Result<(), std::io::Error> {
        self.writer.flush()?;
        self.writer.get_ref().sync_all()
    }

    /// Get the WAL file path.
    pub fn path(&self) -> &str {
        &self.path
    }
}

/// WAL reader for recovery.
pub struct WalReader {
    reader: BufReader<File>,
}

impl WalReader {
    /// Open a WAL file for reading.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let file = File::open(path)?;
        Ok(Self {
            reader: BufReader::new(file),
        })
    }

    /// Read all entries from the WAL.
    pub fn read_all(&mut self) -> Result<Vec<Entry>, std::io::Error> {
        let mut entries = Vec::new();
        
        loop {
            match self.read_entry() {
                Ok(Some(entry)) => entries.push(entry),
                Ok(None) => break,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }
        
        Ok(entries)
    }

    fn read_entry(&mut self) -> Result<Option<Entry>, std::io::Error> {
        // Read type byte
        let mut type_buf = [0u8; 1];
        match self.reader.read_exact(&mut type_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e),
        }

        // Read seq_num
        let mut seq_buf = [0u8; 8];
        self.reader.read_exact(&mut seq_buf)?;
        let seq_num = u64::from_le_bytes(seq_buf);

        // Read key
        let mut key_len_buf = [0u8; 4];
        self.reader.read_exact(&mut key_len_buf)?;
        let key_len = u32::from_le_bytes(key_len_buf) as usize;
        
        let mut key_buf = vec![0u8; key_len];
        self.reader.read_exact(&mut key_buf)?;
        let key = Key::new(key_buf);

        match type_buf[0] {
            WAL_PUT => {
                // Read value
                let mut value_len_buf = [0u8; 4];
                self.reader.read_exact(&mut value_len_buf)?;
                let value_len = u32::from_le_bytes(value_len_buf) as usize;
                
                let mut value_buf = vec![0u8; value_len];
                self.reader.read_exact(&mut value_buf)?;
                let value = Value::new(value_buf);

                Ok(Some(Entry::put(key, seq_num, value)))
            }
            WAL_DELETE => {
                Ok(Some(Entry::delete(key, seq_num)))
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid WAL entry type",
            )),
        }
    }
}

/// Delete a WAL file.
pub fn delete_wal<P: AsRef<Path>>(path: P) -> Result<(), std::io::Error> {
    std::fs::remove_file(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn get_temp_path() -> String {
        let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        format!("/tmp/thordb_wal_test_{}.wal", since_epoch.as_nanos())
    }

    #[test]
    fn test_wal_write_and_read() {
        let path = get_temp_path();

        // Write entries
        {
            let mut wal = Wal::open(&path).unwrap();
            wal.log_put(&Key::from("key1"), &Value::from("value1"), 1).unwrap();
            wal.log_put(&Key::from("key2"), &Value::from("value2"), 2).unwrap();
            wal.log_delete(&Key::from("key1"), 3).unwrap();
        }

        // Read entries
        {
            let mut reader = WalReader::open(&path).unwrap();
            let entries = reader.read_all().unwrap();
            
            assert_eq!(entries.len(), 3);
            
            assert_eq!(entries[0].key.as_bytes(), b"key1");
            assert_eq!(entries[0].seq_num, 1);
            assert!(!entries[0].is_tombstone());
            
            assert_eq!(entries[1].key.as_bytes(), b"key2");
            assert_eq!(entries[1].seq_num, 2);
            
            assert_eq!(entries[2].key.as_bytes(), b"key1");
            assert_eq!(entries[2].seq_num, 3);
            assert!(entries[2].is_tombstone());
        }

        let _ = std::fs::remove_file(path);
    }
}
