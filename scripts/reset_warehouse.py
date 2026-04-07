#!/usr/bin/env python3
# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "requests>=2.25.0"
# ]
# ///

"""Reset Lakekeeper warehouse: drop all tables/namespaces/warehouse, then recreate.

Usage:
    uv run scripts/reset_warehouse.py
    uv run scripts/reset_warehouse.py --drop-only
    uv run scripts/reset_warehouse.py --create-only
"""

import argparse
import logging
import os
import sys
import time

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

MGMT_URL = os.getenv("LAKEKEEPER_MGMT_URL", "http://localhost:8182/management/v1")
PROJECT_ID = os.getenv("LAKEKEEPER_PROJECT_ID", "00000000-0000-0000-0000-000000000000")
WAREHOUSE_NAME = os.getenv("LAKEKEEPER_WAREHOUSE_NAME", "warehouse")

WAREHOUSE_BODY = {
    "warehouse-name": WAREHOUSE_NAME,
    "project-id": PROJECT_ID,
    "storage-profile": {
        "type": "s3",
        "bucket": "lakehouse",
        "endpoint": "http://s3.homelab/",
        "region": "us-east-1",
        "path-style-access": True,
        "flavor": "s3-compat",
        "sts-enabled": False,
    },
    "storage-credential": {
        "type": "s3",
        "credential-type": "access-key",
        "aws-access-key-id": "rustfsadmin",
        "aws-secret-access-key": "rustfsadmin123",
    },
    "delete-profile": {"type": "hard"},
}


def get_warehouse_id() -> str | None:
    resp = requests.get(f"{MGMT_URL}/warehouse")
    resp.raise_for_status()
    for wh in resp.json().get("warehouses", []):
        if wh["name"] == WAREHOUSE_NAME and wh["project-id"] == PROJECT_ID:
            return wh["warehouse-id"]
    return None


def get_catalog_url(warehouse_id: str) -> str:
    base = MGMT_URL.replace("/management/v1", "")
    return f"{base}/catalog/v1"


def drop_warehouse(warehouse_id: str) -> None:
    catalog = get_catalog_url(warehouse_id)
    prefix = warehouse_id

    # List and drop all tables in all namespaces
    ns_resp = requests.get(f"{catalog}/{prefix}/namespaces")
    ns_resp.raise_for_status()
    namespaces = [".".join(ns) for ns in ns_resp.json().get("namespaces", [])]

    for ns in namespaces:
        tbl_resp = requests.get(f"{catalog}/{prefix}/namespaces/{ns}/tables")
        tbl_resp.raise_for_status()
        for ident in tbl_resp.json().get("identifiers", []):
            tbl = ident["name"]
            log.info(f"Dropping table {ns}.{tbl}")
            requests.delete(f"{catalog}/{prefix}/namespaces/{ns}/tables/{tbl}").raise_for_status()

    for ns in namespaces:
        log.info(f"Dropping namespace {ns}")
        requests.delete(f"{catalog}/{prefix}/namespaces/{ns}").raise_for_status()

    # Wait for any pending purge tasks before deleting warehouse
    for attempt in range(12):
        resp = requests.delete(f"{MGMT_URL}/warehouse/{warehouse_id}")
        if resp.status_code in (200, 204):
            log.info(f"Warehouse {warehouse_id} deleted")
            return
        body = resp.json() if resp.content else {}
        err_type = body.get("error", {}).get("type", "")
        if err_type == "WarehouseHasUnfinishedTasks":
            log.info(f"Waiting for purge tasks to finish (attempt {attempt + 1}/12)...")
            time.sleep(5)
        else:
            resp.raise_for_status()

    raise RuntimeError("Timed out waiting for warehouse purge tasks to finish")


def create_warehouse() -> str:
    resp = requests.post(f"{MGMT_URL}/warehouse", json=WAREHOUSE_BODY)
    resp.raise_for_status()
    wh = resp.json()
    log.info(f"Created warehouse '{wh['name']}' id={wh['warehouse-id']}")
    log.info(f"  endpoint : {wh['storage-profile']['endpoint']}")
    log.info(f"  sts      : {wh['storage-profile']['sts-enabled']}")
    return wh["warehouse-id"]


def main() -> int:
    parser = argparse.ArgumentParser(description="Reset Lakekeeper warehouse")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--drop-only", action="store_true", help="Only drop, do not recreate")
    group.add_argument("--create-only", action="store_true", help="Only create (skip drop)")
    args = parser.parse_args()

    if not args.create_only:
        wh_id = get_warehouse_id()
        if wh_id:
            log.info(f"Found warehouse {wh_id}, dropping...")
            drop_warehouse(wh_id)
        else:
            log.info("No existing warehouse found, nothing to drop")

    if not args.drop_only:
        create_warehouse()

    return 0


if __name__ == "__main__":
    sys.exit(main())
