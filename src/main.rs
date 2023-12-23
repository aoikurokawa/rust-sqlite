use std::fs::File;
use std::io::{prelude::*, BufReader};

use anyhow::{bail, Result};

use crate::page::Page;
use crate::record::SerialValue;

mod header;

mod page;

const HEADER_SIZE: usize = 100;

mod record;

mod varint;

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();

    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let command = &args[2];
    let file = File::open(&args[1])?;
    let mut reader = BufReader::new(file);
    let (header, first_page) = Page::read_first_page(&mut reader)?;

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
