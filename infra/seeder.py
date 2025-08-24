from __future__ import annotations

import os
import sys
import requests

from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, LongType, TimestampType


def main() -> int:
    uri = os.environ.get("LAKEKEEPER_URI", "http://lakekeeper:8181/catalog/")
    warehouse = None  # Will be set after getting project ID

    # MinIO credentials for object store access (path-style S3)
    access_key = os.environ.get("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.environ.get("MINIO_ROOT_PASSWORD", "minioadmin")
    region = os.environ.get("AWS_REGION", "us-east-1")
    
    # Detect if running locally (outside Docker) vs inside Docker container
    # When running locally, use localhost URLs; inside Docker use service names
    is_local = uri.startswith("http://localhost:")
    minio_host = "localhost:9000" if is_local else "minio:9000"
    # For PyIceberg S3 configuration, always use localhost when running locally
    s3_endpoint = f"http://{minio_host}"

    print(f"Seeder connecting to {uri} with warehouse {warehouse}")

    # Create warehouse if it doesn't exist
    management_url = uri.replace("/catalog/", "/management/v1/")
    warehouse_data = {
        "warehouse-name": "prod",
        "storage-profile": {
            "type": "s3",
            "bucket": "warehouse",
            "region": region,
            "endpoint": f"http://{minio_host}",
            "path-style-access": True,
            "sts-enabled": False
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": access_key,
            "aws-secret-access-key": secret_key
        }
    }
    
    # Use UUID project ID to match working warehouse
    project_id = "00000000-0000-0000-0000-000000000000"
    print(f"Using default project ID: {project_id}")

    # Bootstrap Lakekeeper to create the default project automatically
    print("Bootstrapping Lakekeeper (creates default project automatically)...")
    try:
        bootstrap_data = {
            "accept-terms-of-use": True,
            "is-operator": True  # Full API access
        }
        bootstrap_resp = requests.post(f"{management_url}bootstrap", json=bootstrap_data)
        if bootstrap_resp.status_code == 204:
            print(f"Lakekeeper bootstrapped successfully, default project created with ID: {project_id}")
        elif bootstrap_resp.status_code == 409:
            print(f"Lakekeeper already bootstrapped, default project exists with ID: {project_id}")
        elif bootstrap_resp.status_code == 400 and "already bootstrapped" in bootstrap_resp.text.lower():
            print(f"Lakekeeper already bootstrapped, default project exists with ID: {project_id}")
        else:
            print(f"Bootstrap failed: {bootstrap_resp.status_code} - {bootstrap_resp.text}")
            return 1
    except Exception as e:
        print(f"Failed to bootstrap Lakekeeper: {e}")
        return 1

    # Use the exact working project's warehouse creation format
    warehouse_data = {
        "warehouse-name": "prod",
        "storage-profile": {
            "type": "s3",
            "bucket": "warehouse",
            "key-prefix": "lakekeeper",
            "region": region,
            "flavor": "aws",
            "endpoint": f"http://{minio_host}",
            "path-style-access": True,
            "sts-enabled": False
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": access_key,
            "aws-secret-access-key": secret_key
        }
    }

    warehouse = f"{project_id}/prod"  # Use project-id/warehouse-name format

    print("Creating warehouse via management API...")
    try:
        headers = {
            "Content-Type": "application/json",
            "x-project-id": project_id
        }
        resp = requests.post(f"{management_url}warehouse", json=warehouse_data, headers=headers)
        if resp.status_code == 201:
            print("Created warehouse 'prod'")
        elif resp.status_code == 409:
            print("Warehouse 'prod' already exists")
        elif resp.status_code == 400 and ("decompress" in resp.text.lower() or "gzip" in resp.text.lower()):
            print("Warehouse has corrupted metadata, continuing anyway (data will be recreated)")
        else:
            print(f"Warehouse creation failed: {resp.status_code} - {resp.text}")
            return 1
    except Exception as e:
        print(f"Failed to create warehouse: {e}")
        return 1

    print("Loading catalog...")
    try:
        # Override S3 configuration to use localhost when running locally
        # Force all S3 operations to use our endpoint configuration
        s3_config = {
            "s3.endpoint": s3_endpoint,
            "s3.path-style-access": True,
            "s3.force-virtual-addressing": False,  # Use path-style addressing 
            "s3.region": region,
            "s3.access-key-id": access_key,
            "s3.secret-access-key": secret_key,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            "s3.connect-timeout": 10,  # Faster timeout for debugging
            "s3.request-timeout": 30,
        }
        
        cat = load_catalog(
            "prod",
            uri=uri,
            warehouse=warehouse,
            **s3_config,
        )
    except Exception as e:
        if "Warehouse" in str(e) and "not found" in str(e):
            print(f"Note: Warehouse creation failed due to storage validation issue (gzip decompression).")
            print(f"This is a known compatibility issue between Lakekeeper and MinIO storage validation.")
            print(f"The warehouse exists but cannot be used due to metadata corruption during creation.")
            print(f"Error: {e}")
            return 1
        else:
            raise

    # Create a couple of namespaces if they don't exist
    for ns in ("analytics", "raw"):
        try:
            if tuple([ns]) not in cat.list_namespaces():
                cat.create_namespace(ns)
                print(f"Created namespace: {ns}")
            else:
                print(f"Namespace already exists: {ns}")
        except Exception as e:  # noqa: BLE001
            print(f"Failed to ensure namespace {ns}: {e}")
            return 1

    # Create simple tables (empty) if missing
    schema = Schema(
        NestedField(1, "event_id", StringType(), required=True),
        NestedField(2, "user_id", LongType(), required=False),
        NestedField(3, "ts", TimestampType(), required=True),
    )

    def ensure_table(ident: str) -> None:
        try:
            cat.load_table(ident)
            print(f"Table exists: {ident}")
        except Exception:
            cat.create_table(ident, schema=schema)
            print(f"Created table: {ident}")

    try:
        ensure_table("analytics.events")
        ensure_table("analytics.sessions")
    except Exception as e:  # noqa: BLE001
        print(f"Failed to ensure tables: {e}")
        return 1

    # Write sample data to tables
    print("Writing sample data...")
    try:
        import pyarrow as pa
        from datetime import datetime, timezone
        
        # Get the exact schema from the table to match field requirements
        events_table = cat.load_table("analytics.events")
        sessions_table = cat.load_table("analytics.sessions")
        
        # Create PyArrow table with schema that matches Iceberg requirements
        events_schema = pa.schema([
            pa.field("event_id", pa.string(), nullable=False),  # Required field
            pa.field("user_id", pa.int64(), nullable=True),     # Optional field  
            pa.field("ts", pa.timestamp('us'), nullable=False)  # Required field
        ])
        
        events_data = pa.Table.from_arrays([
            pa.array(["evt_001", "evt_002", "evt_003", "evt_004", "evt_005"]),
            pa.array([12345, 12346, 12345, 12347, 12346]),
            pa.array([
                datetime(2024, 1, 15, 10, 30, 0),
                datetime(2024, 1, 15, 11, 15, 0), 
                datetime(2024, 1, 15, 14, 45, 0),
                datetime(2024, 1, 16, 9, 20, 0),
                datetime(2024, 1, 16, 16, 30, 0),
            ])
        ], schema=events_schema)
        
        sessions_schema = pa.schema([
            pa.field("event_id", pa.string(), nullable=False),  # Required field
            pa.field("user_id", pa.int64(), nullable=True),     # Optional field
            pa.field("ts", pa.timestamp('us'), nullable=False)  # Required field  
        ])
        
        sessions_data = pa.Table.from_arrays([
            pa.array(["sess_001", "sess_002", "sess_003"]),
            pa.array([12345, 12346, 12347]),
            pa.array([
                datetime(2024, 1, 15, 10, 0, 0),
                datetime(2024, 1, 15, 11, 0, 0),
                datetime(2024, 1, 16, 9, 0, 0),
            ])
        ], schema=sessions_schema)

        # Write data to tables using overwrite for better metadata control
        events_table.overwrite(events_data)
        print(f"Wrote {len(events_data)} records to analytics.events")
        
        sessions_table.overwrite(sessions_data) 
        print(f"Wrote {len(sessions_data)} records to analytics.sessions")
        
    except Exception as e:  # noqa: BLE001
        print(f"Failed to write sample data: {e}")
        return 1

    print("Seeder completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
