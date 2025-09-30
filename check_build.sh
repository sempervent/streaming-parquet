#!/bin/bash

echo "Checking maw build..."

# Clean previous build
cargo clean

# Check dependencies
echo "Checking dependencies..."
cargo check 2>&1 | head -20

echo "Build check complete"
