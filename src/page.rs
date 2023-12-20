use anyhow::{bail, Error};

use crate::{header::Header, record::Record, varint::Varint};

#[repr(u8)]

#[derive(Debug)]

pub enum PageType {

    InteriorIndex = 0x02,

    InteriorTable = 0x05,

    LeafIndex = 0x0a,

    LeafTable = 0x0d,

}

impl TryFrom<u8> for PageType {

    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {

        Ok(match value {

            0x02 => PageType::InteriorIndex,

            0x05 => PageType::InteriorTable,

            0x0a => PageType::LeafIndex,

            0x0d => PageType::LeafTable,

            _ => bail!("Invalid page type"),

        })

    }

}

#[derive(Debug)]

pub struct Page {

    data: Vec<u8>,

    pub page_type: PageType,

    pub freeblock_offset: Option<u16>,

    pub cell_count: u16,

    pub cell_content_offset: u16,

    pub fragmented_free_bytes: u8,

    pub rightmost_pointer: Option<u32>,

    pub cell_offsets: Vec<u16>,

}

impl Page {

}

impl Page {

    pub fn read_first_page<T: std::io::Read>(reader: &mut T) -> anyhow::Result<(Header, Self)> {

        let mut header_data = [0; 100];

        reader.read_exact(&mut header_data)?;

        let header = Header::new(&header_data);

        let mut page = Page::read(header.page_size - 100, reader)?;

        let mut data = header_data.to_vec();

        data.append(&mut page.data);

        page.data = data;

        Ok((header, page))

    }

    pub fn read<T: std::io::Read>(page_size: usize, reader: &mut T) -> anyhow::Result<Self> {

        let mut data = vec![0; page_size];

        reader.read_exact(&mut data)?;

        let page_type = PageType::try_from(data[0])?;

        let freeblock_offset = match u16::from_be_bytes([data[1], data[2]]) {

            0 => None,

            x => Some(x),

        };

        let cell_count = u16::from_be_bytes([data[3], data[4]]);

        let cell_content_offset = u16::from_be_bytes([data[5], data[6]]);

        let fragmented_free_bytes = data[7];

        let rightmost_pointer = match page_type {

            PageType::InteriorIndex | PageType::InteriorTable => {

                Some(u32::from_be_bytes([data[8], data[9], data[10], data[11]]))

            }

            _ => None,

        };

        let mut cell_offsets: Vec<_> = vec![0; cell_count.into()];

        let header_size: u16 = match page_type {

            PageType::InteriorIndex | PageType::InteriorTable => 12,

            _ => 8,

        };

        for i in 0..cell_count {

            let offset = (header_size + i * 2) as usize;

            cell_offsets[i as usize] = u16::from_be_bytes([data[offset], data[offset + 1]]);

        }

        Ok(Self {

            data,

            page_type,

            cell_count,

            cell_content_offset,

            freeblock_offset,

            fragmented_free_bytes,

            rightmost_pointer,

            cell_offsets,

        })

    }

    pub fn read_cell(&self, i: u16) -> anyhow::Result<Record> {

        if i >= self.cell_count {

            bail!("Cell index out of range");

        }

        let offset = self.cell_offsets[i as usize] as usize;

        match self.page_type {

            PageType::LeafTable => {

                let (payload_size, s0) = Varint::read(&self.data, offset);

                let (rowid, s1) = Varint::read(&self.data, offset + s0);

                let payload =

                    &self.data[(offset + s0 + s1)..(offset + s0 + s1 + payload_size.0 as usize)];

                let record = Record::new(payload);

                Ok(record)

            }

            _ => todo!(),

        }

    }

}
