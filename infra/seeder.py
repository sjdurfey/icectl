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

    print(f"Seeder connecting to {uri} with warehouse {warehouse}")

    # Create warehouse if it doesn't exist
    management_url = uri.replace("/catalog/", "/management/v1/")
    warehouse_data = {
        "warehouse-name": "prod",
        "storage-profile": {
            "type": "s3",
            "bucket": "warehouse",
            "region": region,
            "endpoint": "http://minio:9000",
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
    
    # First create project and get project ID
    print("Creating project 'default'...")
    try:
        project_resp = requests.post(f"{management_url}project", json={"project-name": "default"})
        if project_resp.status_code == 201:
            project_data = project_resp.json()
            project_id = project_data["project-id"]
            print(f"Created project 'default' with ID: {project_id}")
        elif project_resp.status_code == 409:
            # Project exists, get project ID
            projects_resp = requests.get(f"{management_url}project")
            projects = projects_resp.json().get("projects", [])
            project_id = None
            for project in projects:
                if project.get("project-name") == "default":
                    project_id = project.get("project-id")
                    break
            if not project_id:
                print("Failed to get project ID for 'default'")
                return 1
            print(f"Project 'default' exists with ID: {project_id}")
        else:
            print(f"Project creation failed: {project_resp.status_code} - {project_resp.text}")
            return 1
    except Exception as e:
        print(f"Failed to create project: {e}")
        return 1

    # Update warehouse data with project ID
    warehouse_data["project-id"] = project_id
    warehouse = f"{project_id}/prod"  # Use project-id/warehouse-name format

    print("Creating warehouse via management API...")
    try:
        resp = requests.post(f"{management_url}warehouse", json=warehouse_data)
        if resp.status_code == 201:
            print("Created warehouse 'prod'")
        elif resp.status_code == 409:
            print("Warehouse 'prod' already exists")
        else:
            print(f"Warehouse creation failed: {resp.status_code} - {resp.text}")
            return 1
    except Exception as e:
        print(f"Failed to create warehouse: {e}")
        return 1

    print("Loading catalog...")
    cat = load_catalog(
        "prod",
        uri=uri,
        warehouse=warehouse,
        **{
            "s3.endpoint": "http://minio:9000",
            "s3.path-style-access": True,
            "s3.region": region,
            "s3.access-key-id": access_key,
            "s3.secret-access-key": secret_key,
        },
    )

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
