use crate::varint::Varint;

#[derive(Debug)]

pub enum SerialType {
    Null,

    I8,

    I16,

    I24,

    I32,

    I48,

    I64,

    Float64,

    Zero,

    One,

    Blob(usize),

    String(usize),
}

impl SerialType {
    pub fn read(int: Varint) -> Self {
        match int.0 {
            0 => Self::Null,

            1 => Self::I8,

            2 => Self::I16,

            3 => Self::I24,

            4 => Self::I32,

            5 => Self::I48,

            6 => Self::I64,

            7 => Self::Float64,

            8 => Self::Zero,

            9 => Self::One,

            n if n >= 12 => {
                if n % 2 == 0 {
                    Self::Blob(((n - 12) / 2) as usize)
                } else {
                    Self::String(((n - 13) / 2) as usize)
                }
            }

            _ => panic!("invalid serial type"),
        }
    }

    pub fn get_length(&self) -> usize {
        match self {
            SerialType::Null => 0,

            SerialType::I8 => 1,

            SerialType::I16 => 2,

            SerialType::I24 => 3,

            SerialType::I32 => 4,

            SerialType::I48 => 6,

            SerialType::I64 => 8,

            SerialType::Float64 => 8,

            SerialType::Zero => 0,

            SerialType::One => 0,

            SerialType::Blob(len) => *len,

            SerialType::String(len) => *len,
        }
    }
}

#[derive(Debug)]

pub enum SerialValue {
    Null,

    I8(i8),

    I16(i16),

    I24(i32),

    I32(i32),

    I48(i64),

    I64(i64),

    Float64(f64),

    Zero,

    One,

    Blob(Vec<u8>),

    String(String),
}

impl SerialValue {
    pub fn unwrap_string(&self) -> String {
        match self {
            SerialValue::String(str) => str.to_owned(),

            _ => panic!(),
        }
    }
}

#[derive(Debug)]

pub struct Record {
    pub serial_types: Vec<SerialType>,

    pub values: Vec<SerialValue>,
}

impl Record {
    pub fn new(data: &[u8]) -> Self {
        let (header_length, hl_size) = Varint::read(data, 0);

        let header_length = header_length.0 as usize;

        let mut header_index = hl_size;

        let mut data_index = header_length;

        let mut serial_types = vec![];

        let mut values = vec![];

        while header_index < header_length {
            let (int, len) = Varint::read(data, header_index);

            header_index += len;

            let serial_type = SerialType::read(int);

            // read data

            let value = match &serial_type {
                SerialType::Null => SerialValue::Null,

                SerialType::I8 => SerialValue::I8(i8::from_be_bytes([data[data_index]])),

                SerialType::I16 => {
                    SerialValue::I16(i16::from_be_bytes([data[data_index], data[data_index + 1]]))
                }

                SerialType::I24 => SerialValue::I24(i32::from_be_bytes([
                    0,
                    data[data_index],
                    data[data_index + 1],
                    data[data_index + 2],
                ])),

                SerialType::I32 => SerialValue::I32(i32::from_be_bytes([
                    data[data_index],
                    data[data_index + 1],
                    data[data_index + 2],
                    data[data_index + 3],
                ])),

                SerialType::I48 => SerialValue::I48(i64::from_be_bytes([
                    0,
                    0,
                    data[data_index],
                    data[data_index + 1],
                    data[data_index + 2],
                    data[data_index + 3],
                    data[data_index + 4],
                    data[data_index + 5],
                ])),

                SerialType::I64 => SerialValue::I64(i64::from_be_bytes([
                    data[data_index],
                    data[data_index + 1],
                    data[data_index + 2],
                    data[data_index + 3],
                    data[data_index + 4],
                    data[data_index + 5],
                    data[data_index + 6],
                    data[data_index + 7],
                ])),

                SerialType::Float64 => SerialValue::Float64(f64::from_be_bytes([
                    data[data_index],
                    data[data_index + 1],
                    data[data_index + 2],
                    data[data_index + 3],
                    data[data_index + 4],
                    data[data_index + 5],
                    data[data_index + 6],
                    data[data_index + 7],
                ])),

                SerialType::Zero => SerialValue::Zero,

                SerialType::One => SerialValue::One,

                SerialType::Blob(len) => {
                    SerialValue::Blob(data[data_index..(data_index + len)].to_vec())
                }

                SerialType::String(len) => SerialValue::String(
                    String::from_utf8(data[data_index..(data_index + len)].to_vec())
                        .expect("invalid utf8"),
                ),
            };

            data_index += serial_type.get_length();

            serial_types.push(serial_type);

            values.push(value);
        }

        Self {
            serial_types,

            values,
        }
    }

    pub fn read_entry(&self, i: usize) {}
}
