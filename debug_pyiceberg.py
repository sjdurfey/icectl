#!/usr/bin/env python3

"""
Debug PyIceberg S3 endpoint configuration
"""

import os
from pyiceberg.catalog import load_catalog

def main():
    # Use same configuration as seeder
    uri = "http://localhost:8181/catalog/"
    warehouse = "00000000-0000-0000-0000-000000000000/prod"
    access_key = "admin"
    secret_key = "password"
    region = "us-east-1"
    
    # Set environment variables that s3fs might use
    os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"
    os.environ["AWS_ACCESS_KEY_ID"] = access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = secret_key
    os.environ["AWS_DEFAULT_REGION"] = region
    
    print(f"Testing PyIceberg catalog connection...")
    print(f"URI: {uri}")
    print(f"Warehouse: {warehouse}")
    
    # Test 1: Basic catalog loading
    try:
        s3_config = {
            "s3.endpoint": "http://localhost:9000",
            "s3.path-style-access": True,
            "s3.force-virtual-addressing": False,
            "s3.region": region,
            "s3.access-key-id": access_key,
            "s3.secret-access-key": secret_key,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        }
        
        print(f"\nS3 Configuration:")
        for key, value in s3_config.items():
            print(f"  {key}: {value}")
        
        cat = load_catalog("test", uri=uri, warehouse=warehouse, **s3_config)
        print("✅ Catalog loaded successfully")
        
        # Test 2: List tables
        tables = cat.list_tables("analytics")
        print(f"✅ Found tables: {tables}")
        
        # Test 3: Load table metadata
        if tables:
            # tables is a list of tuples (namespace, table_name)
            namespace, table_name = tables[0]
            print(f"\nLoading table: {namespace}.{table_name}")
            table = cat.load_table(f"{namespace}.{table_name}")
            print(f"✅ Table loaded: {table}")
            
            # Test 4: Inspect table location and metadata
            print(f"Table location: {table.location()}")
            print(f"Table metadata location: {table.metadata_location}")
            
            # Test 5: Try to read current metadata
            print("\nTrying to access table metadata...")
            try:
                metadata = table.metadata
                print(f"✅ Metadata access successful")
                print(f"Schema: {metadata}")
                
                # Test 6: Try a simple write operation
                print(f"\nTesting write operation...")
                import pyarrow as pa
                from datetime import datetime
                
                # Create simple test data
                test_data = pa.Table.from_arrays([
                    pa.array(["test_event_1"]),
                    pa.array([999]),
                    pa.array([datetime(2024, 1, 1, 12, 0, 0)])
                ], schema=pa.schema([
                    pa.field("event_id", pa.string(), nullable=False),
                    pa.field("user_id", pa.int64(), nullable=True),
                    pa.field("ts", pa.timestamp('us'), nullable=False)
                ]))
                
                # Try to write the data
                table.append(test_data)
                print(f"✅ Write operation successful")
                
            except Exception as e:
                print(f"❌ Write operation failed: {e}")
                import traceback
                traceback.print_exc()
                
    except Exception as e:
        print(f"❌ Failed: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())