use std::path::PathBuf;
use std::{collections::HashSet, str::FromStr};

use clap::{Parser, Subcommand};
use rust_sqlite::{column::SerialValue, database::Database, sql::Sql};

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show status information about the database
    #[clap(name = ".dbinfo")]
    DbInfo { db: PathBuf },

    /// List names of tables matching LIKE pattern TABLE
    #[clap(name = ".tables")]
    Tables { db: PathBuf },

    #[clap(name = ".query")]
    Query { db: PathBuf, statement: String },
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::DbInfo { db } => {
            let db = Database::read_file(db)?;
            println!("database page size: {}", db.page_size());

            if let Some(first_page) = db.pages.get(0) {
                println!("number of tables: {}", first_page.btree_header.ncells());
            }
        }
        Commands::Tables { db } => {
            let db = Database::read_file(db)?;
            match db.pages.get(0) {
                Some(first_page) => {
                    let mut tables = String::new();
                    for i in 0..first_page.btree_header.ncells() {
                        if let Ok((_, Some(record))) = first_page.read_cell(i) {
                            if let SerialValue::String(ref str) = record.columns[0].data() {
                                if str != "table" {
                                    continue;
                                }
                            }

                            let tbl_name = match record.columns[2].data() {
                                SerialValue::String(ref str) => {
                                    if str == "sqlite_sequence" {
                                        continue;
                                    }
                                    &str
                                }
                                _ => "",
                            };

                            tables.push_str(&format!("{} ", tbl_name));
                        };
                    }
                    println!("{tables}");
                }
                None => eprintln!("can not read first page"),
            }
        }
        Commands::Query { db, statement } => {
            let db = Database::read_file(db)?;

            match statement {
                stmt if stmt.to_lowercase().starts_with("select count(*)") => {
                    let select_statement = Sql::from_str(&stmt)?;

                    if let Some(first_page) = db.pages.get(0) {
                        for i in 0..first_page.btree_header.ncells() {
                            if let Ok((_, Some(record))) = first_page.read_cell(i) {
                                match record.columns[0].data() {
                                    SerialValue::String(ref str) => {
                                        if str != "table" {
                                            continue;
                                        }
                                    }
                                    _ => {}
                                }

                                match record.columns[2].data() {
                                    SerialValue::String(str) => match str.as_str() {
                                        "sqlite_sequence" => {
                                            continue;
                                        }
                                        t_name => {
                                            if select_statement.tbl_name == t_name {
                                                match record.columns[3].data() {
                                                    SerialValue::I8(num) => {
                                                        // eprintln!("num: {num}");
                                                        if let Some(page) =
                                                            db.pages.get(*num as usize - 1)
                                                        {
                                                            let cell_len = page.cell_offsets.len();
                                                            println!("{:?}", cell_len);
                                                        }
                                                    }
                                                    _ => {}
                                                }
                                            }
                                        }
                                    },

                                    _ => {}
                                }
                            }
                        }
                    }
                }
                stmt if stmt.to_lowercase().starts_with("select") => {
                    let select_statement = Sql::from_str(&stmt)?;

                    if let Some(first_page) = db.pages.get(0) {
                        for i in (0..first_page.btree_header.ncells()).rev() {
                            match first_page.read_cell(i)? {
                                (_, Some(record)) => {
                                    let mut rowids = HashSet::new();

                                    match record.columns[0].data() {
                                        SerialValue::String(str) => match str.as_str() {
                                            "index" => {
                                                let index_statement = Sql::from_str(
                                                    &record.columns[4].data().display(),
                                                )?;
                                                if let SerialValue::I8(num) =
                                                    record.columns[3].data()
                                                {
                                                    db.read_index(
                                                        *num as usize,
                                                        &index_statement,
                                                        &select_statement,
                                                        &mut rowids,
                                                    );
                                                }
                                                continue;
                                            }
                                            _ => {}
                                        },
                                        _ => {}
                                    }

                                    let mut rowids: Vec<i64> = rowids.into_iter().collect();
                                    rowids.sort_unstable();

                                    match record.columns[2].data() {
                                        SerialValue::String(str) => match str.as_str() {
                                            "sqlite_sequence" => {
                                                continue;
                                            }
                                            t_name => {
                                                if select_statement.tbl_name == t_name {
                                                    match record.columns[3].data() {
                                                        SerialValue::I8(num) => {
                                                            let create_statement = Sql::from_str(
                                                                &record.columns[4].data().display(),
                                                            )?;

                                                            let fields = select_statement
                                                                .get_fields(&create_statement);

                                                            let mut row_set = HashSet::new();
                                                            let mut rowid_set = HashSet::new();

                                                            if rowids.is_empty() {
                                                                db.read_table(
                                                                    *num as usize,
                                                                    &select_statement,
                                                                    fields,
                                                                    &mut row_set,
                                                                    &mut rowid_set,
                                                                );
                                                            } else {
                                                                db.read_ids_from_table(
                                                                    *num as usize,
                                                                    &select_statement,
                                                                    fields,
                                                                    &mut row_set,
                                                                    &mut rowid_set,
                                                                    &rowids,
                                                                );
                                                            }

                                                            row_set
                                                                .iter()
                                                                .for_each(|str| println!("{str}"));
                                                        }
                                                        _ => {}
                                                    }
                                                }
                                            }
                                        },
                                        _ => {}
                                    }
                                }
                                _ => {}
                            }
                        }
                    }
                }
                _ => todo!(),
            }
        }
    }
    Ok(())
}
