#!/bin/bash

echo "Testing maw compilation..."

# Try to compile
cargo check 2>&1 | head -20

echo "Compilation test complete"
