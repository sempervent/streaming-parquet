use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::tempdir;

#[test]
fn test_csv_to_csv_identity() {
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
    assert!(content.contains("a,b,c"));
    assert!(content.contains("1,2,3"));
    assert!(content.contains("4,5,6"));
    assert!(content.contains("7,8,9"));
    assert!(content.contains("10,11,12"));
}

#[test]
fn test_plan_mode() {
    let temp_dir = tempdir().unwrap();
    
    let csv1 = temp_dir.path().join("file1.csv");
    fs::write(&csv1, "a,b,c\n1,2,3\n").unwrap();
    
    let mut cmd = Command::cargo_bin("maw").unwrap();
    let assert = cmd
        .arg("--plan")
        .arg(csv1.to_string_lossy())
        .assert();
    
    assert.success().stdout(predicate::str::contains("Plan mode"));
}

#[test]
fn test_dry_run() {
    let temp_dir = tempdir().unwrap();
    
    let csv1 = temp_dir.path().join("file1.csv");
    fs::write(&csv1, "a,b,c\n1,2,3\n").unwrap();
    
    let mut cmd = Command::cargo_bin("maw").unwrap();
    let assert = cmd
        .arg("--dry-run")
        .arg(csv1.to_string_lossy())
        .assert();
    
    assert.success().stdout(predicate::str::contains("Dry run mode"));
}
