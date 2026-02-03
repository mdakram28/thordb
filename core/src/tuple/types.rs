use std::io::Write;

use crate::tuple::varint::{decode_varint, encode_varint, varint_len};

#[derive(Debug, Clone, PartialEq)]
pub enum TupleFieldType {
    Bool,
    Int32,
    Int64,
    Float32,
    Float64,
    // Variable length bytes
    // Format: [VarInt length] [data ...]
    VarBytes,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TupleValue<'a> {
    Null,
    Bool(bool),
    Int32(i32),
    Int64(i64),
    Float32(f32),
    Float64(f64),
    VarBytes(&'a [u8]),
}

pub struct TupleFieldDescriptor {
    pub name: String,
    pub field_type: TupleFieldType,
}

pub struct TupleDescriptor {
    pub fields: Vec<TupleFieldDescriptor>,
}

impl TupleFieldDescriptor {
    pub fn new(name: String, field_type: TupleFieldType) -> Self {
        Self { name, field_type }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn value_len(&self, data: &[u8]) -> Result<usize, std::io::Error> {
        match self.field_type {
            TupleFieldType::Bool => Ok(1),
            TupleFieldType::Int32 => Ok(4),
            TupleFieldType::Int64 => Ok(8),
            TupleFieldType::Float32 => Ok(4),
            TupleFieldType::Float64 => Ok(8),
            TupleFieldType::VarBytes => {
                let (length, length_len) = decode_varint(data)?;
                Ok(length_len + length as usize)
            },
        }
    }
}

impl TupleDescriptor {
    pub fn new() -> Self {
        Self { fields: Vec::new() }
    }

    pub fn add_field(&mut self, field: TupleFieldDescriptor) {
        self.fields.push(field);
    }

    pub fn get_field(&self, field_index: usize) -> &TupleFieldDescriptor {
        &self.fields[field_index]
    }
}

impl<'a> TupleValue<'a> {
    pub fn len(&self) -> usize {
        match self {
            TupleValue::Null => 0,
            TupleValue::Bool(_) => 1,
            TupleValue::Int32(_) => 4,
            TupleValue::Int64(_) => 8,
            TupleValue::Float32(_) => 4,
            TupleValue::Float64(_) => 8,
            TupleValue::VarBytes(value) => varint_len(value.len() as u64) + value.len(),
        }
    }

    pub fn write_to_stream(&self, stream: &mut impl Write) -> Result<usize, std::io::Error> {
        match self {
            TupleValue::Null => {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Null value not allowed"));
            }
            TupleValue::Bool(value) => {
                stream.write_all(&[*value as u8])?;
                Ok(1)
            }
            TupleValue::Int32(value) => {
                stream.write_all(&value.to_le_bytes())?;
                Ok(4)
            }
            TupleValue::Int64(value) => {
                stream.write_all(&value.to_le_bytes())?;
                Ok(8)
            }
            TupleValue::Float32(value) => {
                stream.write_all(&value.to_le_bytes())?;
                Ok(4)
            }
            TupleValue::Float64(value) => {
                stream.write_all(&value.to_le_bytes())?;
                Ok(8)
            }
            TupleValue::VarBytes(value) => {
                let length = value.len();
                let length_len = encode_varint(length as u64, stream)?;
                stream.write_all(&value)?;
                Ok(length_len + length)
            }
        }
    }
}
