#!/bin/bash

echo "Testing maw build..."

# Try to compile
cargo check 2>&1 | head -30

echo ""
echo "Build test complete"