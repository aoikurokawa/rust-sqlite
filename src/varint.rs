use std::fmt::Display;

pub struct Varint(pub i64);

impl Display for Varint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("{}", self.0))
    }
}

impl Varint {
    pub fn read(data: &[u8], index: usize) -> (Self, usize) {
        let mut result = 0i64;

        let mut size = 0usize;

        // println!("VARINT!");

        for i in 0..9 {
            let byte = data[index + i];

            let has_more = i != 8 && (byte & 0b10000000 != 0);

            // println!("  {i}: {result} {byte:b} {has_more}");

            if has_more {
                result = (result << 7) | ((byte & 0b01111111) as i64);
            } else {
                result = (result << 7) | (byte as i64);

                size = i + 1;

                break;
            }
        }

        // println!(" {result} {size}");

        (Varint(result), size)
    }
}


pub fn decode_varint(bytes: &[u8]) -> anyhow::Result<(i64, usize)> {
    if bytes.is_empty() || bytes.len() > 9 {
        return Err(anyhow::Error::msg(format!(
            "invalid varint: {:?}",
            &bytes[0..9]
        )));
    }

    let mut result = 0;
    let mut shift = 0;
    let mut bytes_read = 0;

    for &byte in bytes.iter().take(8) {
        bytes_read += 1;

        if shift > 64 {
            return Err(anyhow::Error::msg(format!(
                "Varint too long, integer overflow"
            )));
        }

        // (byte & 0x7f):
        // to isolate the lower 7 bits of the byte, effectively removing the high-order bit (the
        // 8th bit) from the byte.
        result |= ((byte & 0x7f) as i64) << shift;
        shift += 7;

        // If the high-order bit of a byte is 0, it signifies the end of the varint, and the
        // function resturns the result
        if byte & 0x80 == 0 {
            return Ok((result, bytes_read));
        }
    }

    // If there are 9 bytes, the function ensures that the ninth byte does not have its high-order
    // bit set ( as per the specification)
    eprintln!("reading 9th bytes");
    if bytes.len() == 9 {
        if let Some(&last_byte) = bytes.get(8) {
            if last_byte & 0x80 != 0 {
                return Err(anyhow::Error::msg(format!(
                    "invalid varint format: {:?}",
                    &bytes[0..9]
                )));
            }
            result |= (last_byte as i64) << shift;
            bytes_read += 1;
        }
    }

    Ok((result, bytes_read))
    // let mut value = 0;

    // for (i, byte) in bytes.iter().enumerate() {
    //     value = (value << (i * 7)) + (byte & 0b0111_1111) as i64;
    //     if byte & 0b1000_0000 == 0 || i > 9 {
    //         return Ok((value, i + 1usize));
    //     }
    // }

    // return Err(anyhow::Error::msg(format!(
    //     "invalid varint: {:?}",
    //     &bytes[0..9]
    // )));
}
