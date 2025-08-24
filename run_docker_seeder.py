#!/usr/bin/env python3

"""
Simple PyIceberg test script to write data inside Docker network
"""

from pyiceberg.catalog import load_catalog
import pyarrow as pa
from datetime import datetime

# Use Docker network addresses
uri = "http://lakekeeper:8181/catalog/"
warehouse = "00000000-0000-0000-0000-000000000000/prod"
access_key = "admin"
secret_key = "password"
region = "us-east-1"

print("🚀 Testing PyIceberg write inside Docker network...")

# Load catalog with Docker network S3 endpoint
s3_config = {
    "s3.endpoint": "http://minio:9000",
    "s3.path-style-access": True,
    "s3.region": region,
    "s3.access-key-id": access_key,
    "s3.secret-access-key": secret_key,
    "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
}

cat = load_catalog("test", uri=uri, warehouse=warehouse, **s3_config)
print("✅ Catalog loaded")

# Load table
table = cat.load_table("analytics.events")
print("✅ Table loaded")

# Create test data
test_data = pa.Table.from_arrays([
    pa.array(["docker_test_1", "docker_test_2", "docker_test_3"]),
    pa.array([100, 200, 300]),
    pa.array([datetime(2024, 1, 1, 12, 0, 0), datetime(2024, 1, 1, 13, 0, 0), datetime(2024, 1, 1, 14, 0, 0)])
], schema=pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("user_id", pa.int64(), nullable=True),
    pa.field("ts", pa.timestamp('us'), nullable=False)
]))

# Write data
table.append(test_data)
print("✅ Data written successfully!")

# Verify data
snapshots = table.current_snapshot()
if snapshots:
    print(f"✅ Current snapshot: {snapshots.snapshot_id}")
else:
    print("❌ No snapshot created")

print("🎉 Docker network seeding completed!")