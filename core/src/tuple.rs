/// Tuple module - defines Tuple and TupleDescriptor for row-based storage

/// Supported column types
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ColumnType {
    Bool,
    Int32,
    Int64,
    Float32,
    Float64,
    /// Fixed-length string (length in bytes)
    FixedString(u16),
    /// Variable-length string (stored as length prefix + data)
    VarString,
}

impl ColumnType {
    /// Returns the fixed size in bytes, or None for variable-length types
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            ColumnType::Bool => Some(1),
            ColumnType::Int32 => Some(4),
            ColumnType::Int64 => Some(8),
            ColumnType::Float32 => Some(4),
            ColumnType::Float64 => Some(8),
            ColumnType::FixedString(len) => Some(*len as usize),
            ColumnType::VarString => None,
        }
    }
}

/// Describes a single column in a tuple
#[derive(Debug, Clone)]
pub struct ColumnDescriptor {
    pub name: String,
    pub column_type: ColumnType,
    pub nullable: bool,
}

/// Describes the schema of a tuple (row)
#[derive(Debug, Clone)]
pub struct TupleDescriptor {
    pub columns: Vec<ColumnDescriptor>,
}

impl TupleDescriptor {
    pub fn new() -> Self {
        Self { columns: Vec::new() }
    }

    pub fn add_column(&mut self, name: &str, column_type: ColumnType, nullable: bool) -> &mut Self {
        self.columns.push(ColumnDescriptor {
            name: name.to_string(),
            column_type,
            nullable,
        });
        self
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }
}

/// A value that can be stored in a tuple column
#[derive(Debug, Clone, PartialEq)]
pub enum TupleValue {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    String(String),
}

impl TupleValue {
    /// Returns the serialized size in bytes for this value
    pub fn serialized_size(&self, column_type: &ColumnType) -> usize {
        match (self, column_type) {
            (TupleValue::Null, _) => 0, // Nulls don't take space (handled by bitmap)
            (TupleValue::Bool(_), ColumnType::Bool) => 1,
            (TupleValue::Int32(_), ColumnType::Int32) => 4,
            (TupleValue::Int64(_), ColumnType::Int64) => 8,
            (TupleValue::Float32(_), ColumnType::Float32) => 4,
            (TupleValue::Float64(_), ColumnType::Float64) => 8,
            (TupleValue::String(_), ColumnType::FixedString(len)) => *len as usize,
            (TupleValue::String(s), ColumnType::VarString) => 4 + s.len(),
            _ => 0,
        }
    }

    /// Serializes the value directly into the buffer, returns bytes written
    pub fn serialize_to(&self, buffer: &mut [u8], column_type: &ColumnType) -> Result<usize, std::io::Error> {
        match (self, column_type) {
            (TupleValue::Null, _) => Ok(0),
            (TupleValue::Bool(v), ColumnType::Bool) => {
                buffer[0] = if *v { 1 } else { 0 };
                Ok(1)
            }
            (TupleValue::Int32(v), ColumnType::Int32) => {
                buffer[..4].copy_from_slice(&v.to_le_bytes());
                Ok(4)
            }
            (TupleValue::Int64(v), ColumnType::Int64) => {
                buffer[..8].copy_from_slice(&v.to_le_bytes());
                Ok(8)
            }
            (TupleValue::Float32(v), ColumnType::Float32) => {
                buffer[..4].copy_from_slice(&v.to_le_bytes());
                Ok(4)
            }
            (TupleValue::Float64(v), ColumnType::Float64) => {
                buffer[..8].copy_from_slice(&v.to_le_bytes());
                Ok(8)
            }
            (TupleValue::String(s), ColumnType::FixedString(len)) => {
                let len = *len as usize;
                let bytes = s.as_bytes();
                let copy_len = bytes.len().min(len);
                buffer[..copy_len].copy_from_slice(&bytes[..copy_len]);
                // Zero-pad the rest
                for b in &mut buffer[copy_len..len] {
                    *b = 0;
                }
                Ok(len)
            }
            (TupleValue::String(s), ColumnType::VarString) => {
                let str_bytes = s.as_bytes();
                let str_len = str_bytes.len() as u32;
                buffer[..4].copy_from_slice(&str_len.to_le_bytes());
                buffer[4..4 + str_bytes.len()].copy_from_slice(str_bytes);
                Ok(4 + str_bytes.len())
            }
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Type mismatch: {:?} vs {:?}", self, column_type),
            )),
        }
    }

    /// Deserializes value from bytes according to column type
    pub fn deserialize(bytes: &[u8], column_type: &ColumnType, is_null: bool) -> Result<(Self, usize), std::io::Error> {
        if is_null {
            return Ok((TupleValue::Null, 0));
        }

        match column_type {
            ColumnType::Bool => {
                if bytes.is_empty() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "not enough data",
                    ));
                }
                Ok((TupleValue::Bool(bytes[0] != 0), 1))
            }
            ColumnType::Int32 => {
                if bytes.len() < 4 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "not enough data",
                    ));
                }
                let value = i32::from_le_bytes(bytes[..4].try_into().unwrap());
                Ok((TupleValue::Int32(value), 4))
            }
            ColumnType::Int64 => {
                if bytes.len() < 8 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "not enough data",
                    ));
                }
                let value = i64::from_le_bytes(bytes[..8].try_into().unwrap());
                Ok((TupleValue::Int64(value), 8))
            }
            ColumnType::Float32 => {
                if bytes.len() < 4 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "not enough data",
                    ));
                }
                let value = f32::from_le_bytes(bytes[..4].try_into().unwrap());
                Ok((TupleValue::Float32(value), 4))
            }
            ColumnType::Float64 => {
                if bytes.len() < 8 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "not enough data",
                    ));
                }
                let value = f64::from_le_bytes(bytes[..8].try_into().unwrap());
                Ok((TupleValue::Float64(value), 8))
            }
            ColumnType::FixedString(len) => {
                let len = *len as usize;
                if bytes.len() < len {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "not enough data",
                    ));
                }
                let s = String::from_utf8_lossy(&bytes[..len])
                    .trim_end_matches('\0')
                    .to_string();
                Ok((TupleValue::String(s), len))
            }
            ColumnType::VarString => {
                if bytes.len() < 4 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "not enough data for length",
                    ));
                }
                let str_len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
                if bytes.len() < 4 + str_len {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "not enough data for string",
                    ));
                }
                let s = String::from_utf8_lossy(&bytes[4..4 + str_len]).to_string();
                Ok((TupleValue::String(s), 4 + str_len))
            }
        }
    }
}

/// A tuple (row) containing values according to a descriptor
#[derive(Debug, Clone)]
pub struct Tuple {
    pub values: Vec<TupleValue>,
}

impl Tuple {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    pub fn with_values(values: Vec<TupleValue>) -> Self {
        Self { values }
    }

    pub fn push(&mut self, value: TupleValue) {
        self.values.push(value);
    }

    /// Returns the serialized size in bytes for this tuple
    pub fn serialized_size(&self, descriptor: &TupleDescriptor) -> usize {
        let null_bitmap_size = (descriptor.columns.len() + 7) / 8;
        let values_size: usize = self
            .values
            .iter()
            .zip(descriptor.columns.iter())
            .filter(|(v, _)| !matches!(v, TupleValue::Null))
            .map(|(v, col)| v.serialized_size(&col.column_type))
            .sum();
        null_bitmap_size + values_size
    }

    /// Serializes the tuple directly into the buffer
    /// Format: [null_bitmap][value1][value2]...
    /// Returns the number of bytes written
    pub fn serialize_to(&self, buffer: &mut [u8], descriptor: &TupleDescriptor) -> Result<usize, std::io::Error> {
        if self.values.len() != descriptor.columns.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Tuple has {} values but descriptor has {} columns",
                    self.values.len(),
                    descriptor.columns.len()
                ),
            ));
        }

        let null_bitmap_size = (descriptor.columns.len() + 7) / 8;

        // Zero out and set null bitmap
        for b in &mut buffer[..null_bitmap_size] {
            *b = 0;
        }
        for (i, value) in self.values.iter().enumerate() {
            if matches!(value, TupleValue::Null) {
                buffer[i / 8] |= 1 << (i % 8);
            }
        }

        let mut offset = null_bitmap_size;

        // Serialize each non-null value
        for (i, value) in self.values.iter().enumerate() {
            if !matches!(value, TupleValue::Null) {
                let written = value.serialize_to(&mut buffer[offset..], &descriptor.columns[i].column_type)?;
                offset += written;
            }
        }

        Ok(offset)
    }

    /// Deserializes a tuple from bytes according to the descriptor
    pub fn deserialize(bytes: &[u8], descriptor: &TupleDescriptor) -> Result<(Self, usize), std::io::Error> {
        let null_bitmap_size = (descriptor.columns.len() + 7) / 8;
        if bytes.len() < null_bitmap_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "not enough data for null bitmap",
            ));
        }

        let null_bitmap = &bytes[..null_bitmap_size];
        let mut offset = null_bitmap_size;
        let mut values = Vec::with_capacity(descriptor.columns.len());

        for (i, col) in descriptor.columns.iter().enumerate() {
            let is_null = (null_bitmap[i / 8] & (1 << (i % 8))) != 0;

            if is_null {
                values.push(TupleValue::Null);
            } else {
                let (value, consumed) = TupleValue::deserialize(&bytes[offset..], &col.column_type, false)?;
                values.push(value);
                offset += consumed;
            }
        }

        Ok((Tuple { values }, offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuple_serialize_deserialize() {
        let mut descriptor = TupleDescriptor::new();
        descriptor
            .add_column("id", ColumnType::Int64, false)
            .add_column("name", ColumnType::VarString, true)
            .add_column("age", ColumnType::Int32, false);

        let tuple = Tuple::with_values(vec![
            TupleValue::Int64(42),
            TupleValue::String("Alice".to_string()),
            TupleValue::Int32(30),
        ]);

        let mut buffer = vec![0u8; tuple.serialized_size(&descriptor)];
        let written = tuple.serialize_to(&mut buffer, &descriptor).unwrap();
        let (deserialized, _) = Tuple::deserialize(&buffer[..written], &descriptor).unwrap();

        assert_eq!(tuple.values, deserialized.values);
    }

    #[test]
    fn test_tuple_with_nulls() {
        let mut descriptor = TupleDescriptor::new();
        descriptor
            .add_column("id", ColumnType::Int64, false)
            .add_column("name", ColumnType::VarString, true)
            .add_column("age", ColumnType::Int32, true);

        let tuple = Tuple::with_values(vec![TupleValue::Int64(1), TupleValue::Null, TupleValue::Int32(25)]);

        let mut buffer = vec![0u8; tuple.serialized_size(&descriptor)];
        let written = tuple.serialize_to(&mut buffer, &descriptor).unwrap();
        let (deserialized, _) = Tuple::deserialize(&buffer[..written], &descriptor).unwrap();

        assert_eq!(tuple.values, deserialized.values);
    }
}
