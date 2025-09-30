use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::tempdir;

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

#[test]
fn test_plan_with_nonexistent_file() {
    let mut cmd = Command::cargo_bin("maw").unwrap();
    let assert = cmd
        .arg("--plan")
        .arg("nonexistent.csv")
        .assert();
    
    // Should fail because file doesn't exist
    assert.failure();
}
