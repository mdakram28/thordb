use std::io::Write;

use crate::tuple::{types::{TupleDescriptor, TupleFieldType, TupleValue}, varint::decode_varint};

pub struct TupleOnDisk<'a> {
    pub data: &'a [u8]
}

pub struct Tuple<'a> {
    pub values: Vec<TupleValue<'a>>,
}


impl<'a> TupleOnDisk<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
        }
    }

    pub fn is_null(&self, field_index: usize) -> bool {
        // Bit is SET when value is null (matches write_to_stream logic)
        self.data[field_index / 8] & (1 << (field_index % 8)) != 0
    }

    pub fn read_field(&self, descriptor: &TupleDescriptor, field_index: usize) -> Result<TupleValue<'a>, std::io::Error> {
        if self.is_null(field_index) {
            return Ok(TupleValue::Null);
        }
        // Skip past null bitmap (rounded up to cover all fields)
        let null_bitmap_len = (descriptor.fields.len() + 7) / 8;
        let mut offset = null_bitmap_len;
        for i in 0..field_index {
            if self.is_null(i) {
                continue;
            }
            let field = descriptor.get_field(i);
            offset += field.value_len(&self.data[offset..])?;
        }
        let field = descriptor.get_field(field_index);
        match field.field_type {
            TupleFieldType::Bool => Ok(TupleValue::Bool(self.data[offset..offset + 1] == [1])),
            TupleFieldType::Int32 => Ok(TupleValue::Int32(i32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap()))),
            TupleFieldType::Int64 => Ok(TupleValue::Int64(i64::from_le_bytes(self.data[offset..offset + 8].try_into().unwrap()))),
            TupleFieldType::Float32 => Ok(TupleValue::Float32(f32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap()))),
            TupleFieldType::Float64 => Ok(TupleValue::Float64(f64::from_le_bytes(self.data[offset..offset + 8].try_into().unwrap()))),
            TupleFieldType::VarBytes => {
                let (length, length_len) = decode_varint(&self.data[offset..])?;
                Ok(TupleValue::VarBytes(&self.data[offset + length_len..offset + length_len + length as usize]))
            },
        }
    }
}

impl<'a> Tuple<'a> {
    pub fn new(values: Vec<TupleValue<'a>>) -> Self {
        Self {
            values,
        }
    }

    pub fn len(&self) -> usize {
        // Null bitmap: round up to cover all fields
        let mut len = (self.values.len() + 7) / 8;
        for value in &self.values {
            len += value.len();
        }
        len
    }

    // pub fn _remove_write_to_disk(&self, disk_tuple: &mut [u8]) -> Result<usize, std::io::Error> {
    //     let mut null_byte = 0 as u8;
    //     for i in 0..self.values.len() {
    //         if let TupleValue::Null = self.values[i] {
    //             null_byte |= 1 << (i % 8);
    //         }
    //         if i % 8 == 7 {
    //             disk_tuple[i / 8] = null_byte;
    //             null_byte = 0;
    //         }
    //     }
    //     let mut offset = 0;
    //     for i in 0..self.values.len() {
    //         if let TupleValue::Null = self.values[i] {
    //             continue;
    //         }
    //         offset += self.values[i].write_bytes(&mut disk_tuple[offset..])?;
    //     }
    //     return Ok(offset);
    // }

    pub fn write_to_stream(&self, stream: &mut impl Write) -> Result<usize, std::io::Error> {
        let mut null_byte = 0 as u8;
        let mut bytes_written = 0;
        for i in 0..self.values.len() {
            if let TupleValue::Null = self.values[i] {
                null_byte |= 1 << (i % 8);
            }
            if i % 8 == 7 {
                stream.write_all(&[null_byte])?;
                bytes_written += 1;
                null_byte = 0;
            }
        }
        if self.values.len() % 8 != 0 {
            stream.write_all(&[null_byte])?;
            bytes_written += 1;
        }

        for value in &self.values {
            // Skip null values - they're only tracked in the null bitmap
            if !matches!(value, TupleValue::Null) {
                bytes_written += value.write_to_stream(stream)?;
            }
        }
        Ok(bytes_written)
    }
}



