use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::tempdir;

#[test]
fn test_csv_concatenation() {
    let temp_dir = tempdir().unwrap();
    
    // Create test CSV files
    let csv1 = temp_dir.path().join("file1.csv");
    let csv2 = temp_dir.path().join("file2.csv");
    let output = temp_dir.path().join("output.csv");
    
    fs::write(&csv1, "a,b,c\n1,2,3\n4,5,6\n").unwrap();
    fs::write(&csv2, "a,b,c\n7,8,9\n10,11,12\n").unwrap();
    
    // Run maw
    let mut cmd = Command::cargo_bin("maw").unwrap();
    let assert = cmd
        .arg(csv1.to_string_lossy())
        .arg(csv2.to_string_lossy())
        .arg("-o")
        .arg(output.to_string_lossy())
        .assert();
    
    assert.success();
    
    // Verify output
    let content = fs::read_to_string(&output).unwrap();
    let lines: Vec<&str> = content.lines().collect();
    
    // Should have header + 4 data rows
    assert_eq!(lines.len(), 5);
    assert_eq!(lines[0], "a,b,c");
    assert!(lines.contains(&"1,2,3"));
    assert!(lines.contains(&"4,5,6"));
    assert!(lines.contains(&"7,8,9"));
    assert!(lines.contains(&"10,11,12"));
}

#[test]
fn test_directory_processing() {
    let temp_dir = tempdir().unwrap();
    let subdir = temp_dir.path().join("subdir");
    fs::create_dir(&subdir).unwrap();
    
    // Create files in subdirectory
    let csv1 = subdir.join("file1.csv");
    let csv2 = subdir.join("file2.csv");
    let output = temp_dir.path().join("output.csv");
    
    fs::write(&csv1, "x,y\n1,2\n").unwrap();
    fs::write(&csv2, "x,y\n3,4\n").unwrap();
    
    // Process directory
    let mut cmd = Command::cargo_bin("maw").unwrap();
    let assert = cmd
        .arg(subdir.to_string_lossy())
        .arg("-o")
        .arg(output.to_string_lossy())
        .assert();
    
    assert.success();
    
    // Verify output
    let content = fs::read_to_string(&output).unwrap();
    assert!(content.contains("x,y"));
    assert!(content.contains("1,2"));
    assert!(content.contains("3,4"));
}

#[test]
fn test_plan_mode() {
    let temp_dir = tempdir().unwrap();
    let csv_file = temp_dir.path().join("test.csv");
    fs::write(&csv_file, "a,b\n1,2\n").unwrap();
    
    let mut cmd = Command::cargo_bin("maw").unwrap();
    let assert = cmd
        .arg("--plan")
        .arg(csv_file.to_string_lossy())
        .assert();
    
    assert.success().stdout(predicate::str::contains("Plan mode"));
}

#[test]
fn test_dry_run() {
    let temp_dir = tempdir().unwrap();
    let csv_file = temp_dir.path().join("test.csv");
    fs::write(&csv_file, "a,b\n1,2\n").unwrap();
    
    let mut cmd = Command::cargo_bin("maw").unwrap();
    let assert = cmd
        .arg("--dry-run")
        .arg(csv_file.to_string_lossy())
        .assert();
    
    assert.success().stdout(predicate::str::contains("Dry run mode"));
}

#[test]
fn test_no_inputs() {
    let mut cmd = Command::cargo_bin("maw").unwrap();
    let assert = cmd.assert();
    
    // Should fail because no inputs provided
    assert.failure();
}

#[test]
fn test_help() {
    let mut cmd = Command::cargo_bin("maw").unwrap();
    let assert = cmd.arg("--help").assert();
    
    assert.success().stdout(predicate::str::contains("maw"));
}

#[test]
fn test_version() {
    let mut cmd = Command::cargo_bin("maw").unwrap();
    let assert = cmd.arg("--version").assert();
    
    assert.success().stdout(predicate::str::contains("maw"));
}
