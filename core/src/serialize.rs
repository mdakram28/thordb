use crate::page::BinarySerializable;

impl BinarySerializable for u64 {
    fn write_to(&self, buffer: &mut [u8]) {
        let bytes = self.to_le_bytes();
        buffer[..8].copy_from_slice(&bytes);
    }

    fn read_from(buffer: &[u8]) -> Result<Self, std::io::Error> {
        if buffer.len() < 8 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "buffer too small"));
        }
        let bytes: [u8; 8] = buffer[..8].try_into().unwrap();
        Ok(u64::from_le_bytes(bytes))
    }
}

impl BinarySerializable for u32 {
    fn write_to(&self, buffer: &mut [u8]) {
        let bytes = self.to_le_bytes();
        buffer[..4].copy_from_slice(&bytes);
    }

    fn read_from(buffer: &[u8]) -> Result<Self, std::io::Error> {
        if buffer.len() < 4 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "buffer too small"));
        }
        let bytes: [u8; 4] = buffer[..4].try_into().unwrap();
        Ok(u32::from_le_bytes(bytes))
    }
}
