use std::process::{Command, Output};

fn build_command(db_name: &str) -> Output {
    Command::new("./target/debug/rust-sqlite")
        .arg(".tables")
        .arg(db_name)
        .output()
        .expect("failed to execute .tables process")
}

#[test]
fn test_cli_tables_sample_db() {
    let output = build_command("sample.db");
    let stdout = String::from_utf8(output.stdout).expect("parse to String");

    assert!(stdout.contains("apples oranges"));
    assert!(output.status.success());
}

#[test]
fn test_cli_tables_superheroes_db() {
    let output = build_command("superheroes.db");
    let stdout = String::from_utf8(output.stdout).expect("parse to String");

    assert!(stdout.contains("superheroes"));
    assert!(output.status.success());
}

#[test]
fn test_cli_tables_companies_db() {
    let output = build_command("companies.db");
    let stdout = String::from_utf8(output.stdout).expect("parse to String");

    assert!(stdout.contains("companies"));
    assert!(output.status.success());
}
