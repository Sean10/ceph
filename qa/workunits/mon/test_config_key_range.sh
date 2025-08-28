#!/bin/bash
#
# Test script for config-key rm-range functionality
# This script tests the new range deletion functionality in KVMonitor
#
set -e

# Helper function to run ceph command and capture output
run_ceph() {
    echo "Running: ceph $@"
    ceph "$@" 2>&1
}

# Helper function to check if key exists
key_exists() {
    local key="$1"
    if ceph config-key exists "$key" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Setup test data
echo "=== Setting up test data ==="
run_ceph config-key set "test/range/key1" "value1"
run_ceph config-key set "test/range/key2" "value2" 
run_ceph config-key set "test/range/key3" "value3"
run_ceph config-key set "test/range/subdir/key4" "value4"
run_ceph config-key set "test/range/subdir/key5" "value5"
run_ceph config-key set "test/other/key6" "value6"

# List all test keys
echo "=== Initial state ==="
run_ceph config-key ls | grep "test/"

# Test 1: Range deletion with start and end bounds
echo "=== Test 1: Range deletion [test/range/key1, test/range/key3) ==="
run_ceph config-key rm-range "test/range" "key1" "key3"

# Check that key1 and key2 are deleted, but key3 and others remain
echo "Checking results..."
if key_exists "test/range/key1"; then
    echo "ERROR: key1 should be deleted"
    exit 1
fi
if key_exists "test/range/key2"; then
    echo "ERROR: key2 should be deleted" 
    exit 1
fi
if ! key_exists "test/range/key3"; then
    echo "ERROR: key3 should still exist"
    exit 1
fi
if ! key_exists "test/range/subdir/key4"; then
    echo "ERROR: key4 should still exist"
    exit 1
fi

echo "Test 1 PASSED"

# Test 2: Prefix deletion (remove all keys with prefix)
echo "=== Test 2: Prefix deletion (all test/range keys) ==="
run_ceph config-key rm-range "test/range"

# Check that all test/range keys are deleted
echo "Checking results..."
if key_exists "test/range/key3"; then
    echo "ERROR: key3 should be deleted"
    exit 1
fi
if key_exists "test/range/subdir/key4"; then
    echo "ERROR: key4 should be deleted"
    exit 1
fi
if key_exists "test/range/subdir/key5"; then
    echo "ERROR: key5 should be deleted"
    exit 1
fi
if ! key_exists "test/other/key6"; then
    echo "ERROR: key6 should still exist"
    exit 1
fi

echo "Test 2 PASSED"

# Test 3: Range deletion on non-existent keys (should not error)
echo "=== Test 3: Range deletion on non-existent keys ==="
run_ceph config-key rm-range "nonexistent/prefix" "start" "end"
echo "Test 3 PASSED"

# Test 4: Empty range (start == end, should delete nothing)
echo "=== Test 4: Empty range deletion ==="
run_ceph config-key set "test/empty/key1" "value1"
run_ceph config-key rm-range "test/empty" "key1" "key1"
if ! key_exists "test/empty/key1"; then
    echo "ERROR: key1 should still exist (empty range)"
    exit 1
fi
echo "Test 4 PASSED"

# Cleanup
echo "=== Cleanup ==="
run_ceph config-key rm-range "test"

echo "=== All tests PASSED ==="