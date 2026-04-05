#!/usr/bin/env python3
"""
Test script to verify the branch listing fix works correctly.
This creates mock table metadata to test the get_table_branches function.
"""

import sys
sys.path.insert(0, 'src')

from icelib.clients import get_table_branches
from icelib.config import Catalog

# Mock classes to simulate PyIceberg objects
class MockSnapshot:
    def __init__(self, snapshot_id, parent_snapshot_id=None, summary=None):
        self.snapshot_id = snapshot_id
        self.parent_snapshot_id = parent_snapshot_id
        self.summary = summary or {}

class MockRef:
    def __init__(self, ref_type, snapshot_id):
        self.type = ref_type
        self.snapshot_id = snapshot_id

class MockMetadata:
    def __init__(self, current_snapshot_id, snapshots=None, refs=None):
        self.current_snapshot_id = current_snapshot_id
        self.snapshots = snapshots or []
        self.refs = refs or {}

class MockTable:
    def __init__(self, metadata):
        self.metadata = metadata

class MockCatalog:
    def load_table(self, table_name):
        # Create test data with multiple branches and tags
        snapshots = [
            MockSnapshot(1001, None, {"operation": "append", "total-records": "100"}),
            MockSnapshot(1002, 1001, {"operation": "append", "total-records": "200"}), 
            MockSnapshot(1003, 1002, {"operation": "append", "total-records": "300"}),
            MockSnapshot(1004, 1002, {"operation": "append", "total-records": "250"}),  # branch from 1002
        ]
        
        refs = {
            "main": MockRef("branch", 1003),
            "dev": MockRef("branch", 1004), 
            "v1.0": MockRef("tag", 1002),
            "feature/xyz": MockRef("branch", 1001)
        }
        
        metadata = MockMetadata(
            current_snapshot_id=1003,  # main is current
            snapshots=snapshots,
            refs=refs
        )
        
        return MockTable(metadata)

# Mock the load_catalog function
def mock_load_catalog(name, **props):
    return MockCatalog()

# Patch the import
import icelib.clients
original_load_catalog = None

def patch_load_catalog():
    global original_load_catalog
    import pyiceberg.catalog
    original_load_catalog = pyiceberg.catalog.load_catalog
    pyiceberg.catalog.load_catalog = mock_load_catalog

def unpatch_load_catalog():
    if original_load_catalog:
        import pyiceberg.catalog
        pyiceberg.catalog.load_catalog = original_load_catalog

def test_branch_listing():
    """Test the enhanced branch listing functionality."""
    print("Testing enhanced branch listing...")
    
    # Create a mock catalog config
    catalog = Catalog(name="test", type="rest", uri="http://test", warehouse="test")
    
    # Patch the load_catalog function
    patch_load_catalog()
    
    try:
        # Test the function
        branches = get_table_branches(catalog, "test.table")
        
        print(f"Found {len(branches)} branches/refs:")
        for branch in branches:
            current_marker = " [CURRENT]" if branch["is_current"] else ""
            print(f"  - {branch['name']} ({branch['type']}): {branch['snapshot_id']}{current_marker}")
            print(f"    Parent: {branch['parent_ref']}, Action: {branch['action']}")
        
        # Verify we have the expected refs
        branch_names = [b["name"] for b in branches]
        expected_refs = ["main", "dev", "v1.0", "feature/xyz"]
        
        print(f"\nExpected refs: {expected_refs}")
        print(f"Found refs: {branch_names}")
        
        # Check if all expected refs are present
        missing_refs = set(expected_refs) - set(branch_names)
        if missing_refs:
            print(f"❌ Missing refs: {missing_refs}")
            return False
        
        # Check if current branch is correctly identified
        current_branches = [b for b in branches if b["is_current"]]
        if len(current_branches) != 1 or current_branches[0]["name"] != "main":
            print(f"❌ Current branch detection failed. Found: {[b['name'] for b in current_branches]}")
            return False
        
        # Check if branches are sorted correctly (current first, then alphabetical)
        if branches[0]["name"] != "main":
            print(f"❌ Sorting failed. First branch should be 'main', got '{branches[0]['name']}'")
            return False
            
        print("✅ All tests passed! Branch listing fix works correctly.")
        return True
        
    finally:
        unpatch_load_catalog()

if __name__ == "__main__":
    success = test_branch_listing()
    sys.exit(0 if success else 1)