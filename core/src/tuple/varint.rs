use std::io::Write;

pub(crate) fn varint_len(mut value: u64) -> usize {
    let mut len = 0;
    while value >= 0x80 {
        len += 1;
        value >>= 7;
    }
    len + 1
}

pub(crate) fn encode_varint(mut value: u64, stream: &mut impl Write) -> Result<usize, std::io::Error> {
    let mut bytes_written = 0;
    while value >= 0x80 {
        stream.write_all(&[((value & 0x7F) | 0x80) as u8])?;
        bytes_written += 1;
        value >>= 7;
    }
    stream.write_all(&[value as u8])?;
    bytes_written += 1;
    Ok(bytes_written)
}

pub(crate) fn decode_varint(bytes: &[u8]) -> Result<(u64, usize), std::io::Error> {
    let mut value = 0u64;
    let mut shift = 0;
    let mut bytes_read = 0;

    for &byte in bytes {
        bytes_read += 1;
        value |= ((byte & 0x7F) as u64) << shift;
        if (byte & 0x80) == 0 {
            return Ok((value, bytes_read));
        }

        shift += 7;
        if shift >= 64 {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Varint too large!"));
        }
    }
    Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Varint not terminated!"))
}