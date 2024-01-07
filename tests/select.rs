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
fn test_cli_select_single_field_sample_db() {
    let output = build_select_field_command("sample.db", "SELECT name FROM apples");
    let stdout = String::from_utf8(output.stdout).expect("parse to String");
    let expects = vec!["Granny Smith", "Fuji", "Honeycrisp", "Golden Delicious"];

    let outputs: Vec<&str> = stdout.lines().collect();

    assert_eq!(expects, outputs);
    assert!(output.status.success());
}

/*
*
* SELECT `field_name`, `field_name` FROM `table`
*
* **/
#[test]
fn test_cli_select_multiple_field_sample_db() {
    let output = build_select_field_command("sample.db", "SELECT name, color FROM apples");
    let stdout = String::from_utf8(output.stdout).expect("parse to String");
    let expects = vec![
        "Granny Smith|Light Green",
        "Fuji|Red",
        "Honeycrisp|Blush Red",
        "Golden Delicious|Yellow",
    ];

    let outputs: Vec<&str> = stdout.lines().collect();

    assert_eq!(expects, outputs);
    assert!(output.status.success());
}

/*
*
* SELECT `field_name`, `field_name` FROM `table` WHERE ...
*
* **/
#[test]
fn test_cli_select_multiple_field_with_where_sample_db() {
    let output = build_select_field_command(
        "sample.db",
        "SELECT name, color FROM apples WHERE color = 
'Yellow'",
    );
    let stdout = String::from_utf8(output.stdout).expect("parse to String");
    let expects = vec!["Golden Delicious|Yellow"];

    let outputs: Vec<&str> = stdout.lines().collect();

    assert_eq!(expects, outputs);
    assert!(output.status.success());
}

/*
*
* SELECT `field_name`, 'field_name` FROM `table` WHERE ...
*
* **/
#[test]
fn test_cli_select_multiple_field_with_where_superheroes_db() {
    let output = build_select_field_command(
        "superheroes.db",
        "SELECT id, name FROM superheroes WHERE eye_color = 
'Pink Eyes'",
    );
    let stdout = String::from_utf8(output.stdout).expect("parse to String");
    let mut expects = vec![
        "297|Stealth (New Earth)",
        "1085|Felicity (New Earth)",
        "3913|Matris Ater Clementia (New Earth)",
        "3289|Angora Lapin (New Earth)",
        "790|Tobias Whale (New Earth)",
        "2729|Thrust (New Earth)",
    ];

    let mut outputs: Vec<&str> = stdout.lines().collect();

    expects.sort();
    outputs.sort();

    assert_eq!(expects, outputs);
    assert!(output.status.success());
}
