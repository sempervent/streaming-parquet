#!/bin/bash

# Create test data
mkdir -p test_data
echo "id,name,value" > test_data/file1.csv
echo "1,Alice,100" >> test_data/file1.csv
echo "2,Bob,200" >> test_data/file1.csv

echo "id,name,value" > test_data/file2.csv
echo "3,Charlie,300" >> test_data/file2.csv
echo "4,Diana,400" >> test_data/file2.csv

# Basic concatenation
echo "=== Basic CSV concatenation ==="
cargo run --release -- test_data/*.csv -o output.csv

echo "=== Output ==="
cat output.csv

# Plan mode
echo "=== Plan mode ==="
cargo run --release -- test_data/ --plan

# Cleanup
rm -rf test_data output.csv
