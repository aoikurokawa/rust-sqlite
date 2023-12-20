pub struct Header {

    pub page_size: usize,

}

impl Header {

    pub fn new(data: &[u8]) -> Self {

        let page_size = u16::from_be_bytes([data[16], data[17]]) as usize;

        Self { page_size }

    }

}
