use anyhow::{bail, Result};

use std::fs::File;

use std::io::{prelude::*, BufReader};

use crate::page::Page;

use crate::record::SerialValue;

mod header;

mod page;

const HEADER_SIZE: usize = 100;

mod record;

mod varint;

fn main() -> Result<()> {

    // Parse arguments

    let args = std::env::args().collect::<Vec<_>>();

    match args.len() {

        0 | 1 => bail!("Missing <database path> and <command>"),

        2 => bail!("Missing <command>"),

        _ => {}

    }

    // Parse command and act accordingly

    let command = &args[2];

    match command.as_str() {

        ".dbinfo" => {

            let file = File::open(&args[1])?;

            let mut reader = BufReader::new(file);

            let mut header = [0; HEADER_SIZE];

            reader.read_exact(&mut header)?;

            // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order

            #[allow(unused_variables)]

            let page_size = u16::from_be_bytes([header[16], header[17]]) as usize;

            let first_page = Page::read(page_size - HEADER_SIZE, &mut reader)?;

    let file = File::open(&args[1])?;

    let mut reader = BufReader::new(file);

    let (header, first_page) = Page::read_first_page(&mut reader)?;

            // println!("{page:?}");

            // You can use print statements as follows for debugging, they'll be visible when running tests.

            println!("database page size: {page_size}");

    match command.as_str() {

        ".dbinfo" => {

            println!("database page size: {}", header.page_size);

            println!("number of tables: {}", first_page.cell_count);

        }

        ".tables" => {

            println!("{:?}", first_page.cell_offsets);

            for i in 0..first_page.cell_count {

                let record = first_page.read_cell(i)?;

                if &record.values[0].unwrap_string() != "table" {

                    continue;

                }

                let tbl_name = record.values[2].unwrap_string();

                if tbl_name == "sqlite_sequence" {

                    continue;

                }

                print!("{tbl_name} ");

            }

            println!()

        }

        _ => bail!("Missing or invalid command passed: {}", command),

    }

    Ok(())

}
