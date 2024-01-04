use std::process::{Command, Output};

/*
*
* SELECT COUNT(*) FROM `table`
*
* **/
fn build_select_count_command(db_name: &str, statement: &str) -> Output {
    Command::new("./target/debug/rust-sqlite")
        .arg(".query")
        .arg(db_name)
        .arg(statement)
        .output()
        .expect("failed to execute .tables process")
}

#[test]
fn test_cli_select_count_sample_db() {
    let output = build_select_count_command("sample.db", "SELECT COUNT(*) FROM apples");
    let stdout = String::from_utf8(output.stdout).expect("parse to String");

    assert!(stdout.contains("4"));
    assert!(output.status.success());
}

#[test]
fn test_cli_select_count_superheroes_db() {
    let output = build_select_count_command("superheroes.db", "SELECT COUNT(*) FROM superheroes");
    let stdout = String::from_utf8(output.stdout).expect("parse to String");

    assert!(stdout.contains("108"));
    assert!(output.status.success());
}

#[test]
fn test_cli_select_count_companies_db() {
    let output = build_select_count_command("companies.db", "SELECT COUNT(*) FROM companies");
    let stdout = String::from_utf8(output.stdout).expect("parse to String");

    assert!(stdout.contains("4"));
    assert!(output.status.success());
}

/*
*
* SELECT `field_name` FROM `table`
*
* **/
fn build_select_field_command(db_name: &str, statement: &str) -> Output {
    Command::new("./target/debug/rust-sqlite")
        .arg(".query")
        .arg(db_name)
        .arg(statement)
        .output()
        .expect("failed to execute .tables process")
}

#[test]
fn test_cli_tables_sample_db() {
    let output = build_select_field_command("sample.db", "SELECT name FROM apples");
    let stdout = String::from_utf8(output.stdout).expect("parse to String");

    assert!(stdout.contains(
        "Granny Smith
 Fuji
 Honeycrisp
 Golden Delicious"
    ));
    assert!(output.status.success());
}

#[test]
fn test_cli_tables_superheroes_db() {
    let output = build_select_field_command("superheroes.db", "SELECT name FROM superheroes");
    let stdout = String::from_utf8(output.stdout).expect("parse to String");

    assert!(stdout.contains("108"));
    assert!(output.status.success());
}

#[test]
fn test_cli_tables_companies_db() {
    let output = build_select_field_command("companies.db", "SELECT COUNT(*) FROM companies");
    let stdout = String::from_utf8(output.stdout).expect("parse to String");

    assert!(stdout.contains("4"));
    assert!(output.status.success());
}
