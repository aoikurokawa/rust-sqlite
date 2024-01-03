use std::collections::HashSet;
use std::path::PathBuf;

use anyhow::bail;
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
                            match record.columns[0].data() {
                                SerialValue::String(ref str) => {
                                    if str != "table" {
                                        continue;
                                    }
                                }
                                _ => {}
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
        Commands::Query { db, statement } => {}
    }
    Ok(())
}
