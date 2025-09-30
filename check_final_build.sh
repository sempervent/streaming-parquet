#!/bin/bash

echo "Final build check for maw..."

# Try to compile
cargo check 2>&1 | head -30

echo ""
echo "Final build check complete"