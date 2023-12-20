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
