use std::process::{Command, Output};

fn build_command(db_name: &str) -> Output {
    Command::new("./target/debug/rust-sqlite")
        .arg(".dbinfo")
        .arg(db_name)
        .output()
        .expect("failed to execute .dbinfo process")
}

#[test]
fn test_cli_dbinfo_sample_db() {
    let output = build_command("sample.db");
    let stdout = String::from_utf8(output.stdout).expect("parse to String");

    assert!(stdout.contains(
        "database page size: 4096
number of tables: 3"
    ));
    assert!(output.status.success());
}

#[test]
fn test_cli_dbinfo_superheroes_db() {
    let output = build_command("superheroes.db");
    let stdout = String::from_utf8(output.stdout).expect("parse to String");

    assert!(stdout.contains(
        "database page size: 4096
number of tables: 2"
    ));
    assert!(output.status.success());
}

#[test]
fn test_cli_dbinfo_companies_db() {
    let output = build_command("companies.db");
    let stdout = String::from_utf8(output.stdout).expect("parse to String");

    assert!(stdout.contains(
        "database page size: 4096
number of tables: 3"
    ));
    assert!(output.status.success());
}
