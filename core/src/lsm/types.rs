//! Core types for the LSM tree.

use std::cmp::Ordering;
use std::io::Write;

use crate::tuple::varint::{decode_varint, encode_varint, varint_len};

/// Sequence number for ordering entries with the same key.
/// Higher sequence numbers are newer.
pub type SeqNum = u64;

/// Key type - variable length bytes.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct Key(pub Vec<u8>);

impl Key {
    pub fn new(data: Vec<u8>) -> Self {
        Self(data)
    }

    pub fn from_slice(data: &[u8]) -> Self {
        Self(data.to_vec())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.cmp(&other.0)
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl From<&[u8]> for Key {
    fn from(data: &[u8]) -> Self {
        Self::from_slice(data)
    }
}

impl From<&str> for Key {
    fn from(s: &str) -> Self {
        Self::from_slice(s.as_bytes())
    }
}

impl From<Vec<u8>> for Key {
    fn from(data: Vec<u8>) -> Self {
        Self::new(data)
    }
}

/// Value type - variable length bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Value(pub Vec<u8>);

impl Value {
    pub fn new(data: Vec<u8>) -> Self {
        Self(data)
    }

    pub fn from_slice(data: &[u8]) -> Self {
        Self(data.to_vec())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl From<&[u8]> for Value {
    fn from(data: &[u8]) -> Self {
        Self::from_slice(data)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Self::from_slice(s.as_bytes())
    }
}

impl From<Vec<u8>> for Value {
    fn from(data: Vec<u8>) -> Self {
        Self::new(data)
    }
}

/// An entry in the LSM tree.
/// 
/// - `key`: The key bytes
/// - `seq_num`: Sequence number for ordering (higher = newer)
/// - `value`: Some(value) for a put, None for a delete (tombstone)
#[derive(Clone, Debug)]
pub struct Entry {
    pub key: Key,
    pub seq_num: SeqNum,
    pub value: Option<Value>,
}

impl Entry {
    /// Create a new put entry.
    pub fn put(key: Key, seq_num: SeqNum, value: Value) -> Self {
        Self {
            key,
            seq_num,
            value: Some(value),
        }
    }

    /// Create a new delete entry (tombstone).
    pub fn delete(key: Key, seq_num: SeqNum) -> Self {
        Self {
            key,
            seq_num,
            value: None,
        }
    }

    /// Returns true if this is a tombstone (delete marker).
    pub fn is_tombstone(&self) -> bool {
        self.value.is_none()
    }

    /// Serialized size in bytes.
    pub fn serialized_size(&self) -> usize {
        // Format: key_len (varint) + key + seq_num (8) + tombstone (1) + [value_len (varint) + value]
        let mut size = varint_len(self.key.len() as u64) + self.key.len();
        size += 8; // seq_num
        size += 1; // tombstone flag
        if let Some(ref value) = self.value {
            size += varint_len(value.len() as u64) + value.len();
        }
        size
    }

    /// Serialize entry to a writer.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> Result<usize, std::io::Error> {
        let mut written = 0;

        // Write key length + key
        written += encode_varint(self.key.len() as u64, writer)?;
        writer.write_all(self.key.as_bytes())?;
        written += self.key.len();

        // Write sequence number
        writer.write_all(&self.seq_num.to_le_bytes())?;
        written += 8;

        // Write tombstone flag and value
        if let Some(ref value) = self.value {
            writer.write_all(&[0u8])?; // not a tombstone
            written += 1;
            written += encode_varint(value.len() as u64, writer)?;
            writer.write_all(value.as_bytes())?;
            written += value.len();
        } else {
            writer.write_all(&[1u8])?; // tombstone
            written += 1;
        }

        Ok(written)
    }

    /// Deserialize entry from bytes.
    pub fn read_from(data: &[u8]) -> Result<(Self, usize), std::io::Error> {
        let mut offset = 0;

        // Read key length + key
        let (key_len, key_len_size) = decode_varint(&data[offset..])?;
        offset += key_len_size;
        let key = Key::from_slice(&data[offset..offset + key_len as usize]);
        offset += key_len as usize;

        // Read sequence number
        let seq_num = u64::from_le_bytes(
            data[offset..offset + 8]
                .try_into()
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid seq_num"))?
        );
        offset += 8;

        // Read tombstone flag
        let is_tombstone = data[offset] != 0;
        offset += 1;

        // Read value if not tombstone
        let value = if is_tombstone {
            None
        } else {
            let (value_len, value_len_size) = decode_varint(&data[offset..])?;
            offset += value_len_size;
            let value = Value::from_slice(&data[offset..offset + value_len as usize]);
            offset += value_len as usize;
            Some(value)
        };

        Ok((
            Self {
                key,
                seq_num,
                value,
            },
            offset,
        ))
    }
}

/// Ordering for entries: first by key ascending, then by seq_num descending.
/// This ensures that for the same key, newer entries come first.
impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Equal => other.seq_num.cmp(&self.seq_num), // Descending seq_num
            ord => ord,
        }
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for Entry {}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.seq_num == other.seq_num
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_serialization() {
        let entry = Entry::put(
            Key::from("hello"),
            42,
            Value::from("world"),
        );

        let mut buffer = Vec::new();
        let written = entry.write_to(&mut buffer).unwrap();
        assert_eq!(written, buffer.len());

        let (decoded, read) = Entry::read_from(&buffer).unwrap();
        assert_eq!(read, buffer.len());
        assert_eq!(decoded.key, entry.key);
        assert_eq!(decoded.seq_num, entry.seq_num);
        assert_eq!(decoded.value, entry.value);
    }

    #[test]
    fn test_tombstone_serialization() {
        let entry = Entry::delete(Key::from("deleted"), 100);

        let mut buffer = Vec::new();
        entry.write_to(&mut buffer).unwrap();

        let (decoded, _) = Entry::read_from(&buffer).unwrap();
        assert!(decoded.is_tombstone());
        assert_eq!(decoded.key, entry.key);
        assert_eq!(decoded.seq_num, entry.seq_num);
    }

    #[test]
    fn test_entry_ordering() {
        let e1 = Entry::put(Key::from("a"), 1, Value::from("v1"));
        let e2 = Entry::put(Key::from("a"), 2, Value::from("v2"));
        let e3 = Entry::put(Key::from("b"), 1, Value::from("v3"));

        // Same key: higher seq_num comes first
        assert!(e2 < e1);
        // Different keys: ordered by key
        assert!(e1 < e3);
    }
}
