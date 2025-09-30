#!/bin/bash

echo "Checking maw compilation status..."

# Try to compile and capture output
cargo check 2>&1 | head -30

echo ""
echo "Build check complete"
