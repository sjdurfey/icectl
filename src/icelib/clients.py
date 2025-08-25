from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

from .config import Catalog


def _build_pyiceberg_properties(cat: Catalog) -> Dict[str, object]:
    """Build properties dict for pyiceberg.catalog.load_catalog.

    Includes common fields derived from our Catalog plus any user-provided overrides.
    """
    props: Dict[str, object] = {}
    if cat.uri:
        props["uri"] = cat.uri
    if cat.warehouse:
        props["warehouse"] = cat.warehouse
    # Forward overrides verbatim (already env-expanded by config loader)
    props.update(cat.overrides or {})
    return props


def _ns_to_str(ns_ident: Iterable[str]) -> str:
    return ".".join(part for part in ns_ident if part)


def list_namespaces(cat: Catalog) -> List[str]:
    """Return namespace names as strings for a given catalog.

    This function imports PyIceberg lazily to keep unit tests light. It expects
    the catalog type to be resolvable by `pyiceberg.catalog.load_catalog`.
    """
    from pyiceberg.catalog import load_catalog  # local import: optional for tests

    props = _build_pyiceberg_properties(cat)
    ice_catalog = load_catalog(cat.name, **props)
    namespaces = ice_catalog.list_namespaces()  # returns identifiers (tuples)
    return sorted(_ns_to_str(ns) for ns in namespaces)


def _to_namespace_tuple(namespace: str | Iterable[str]) -> Tuple[str, ...]:
    if isinstance(namespace, str):
        return tuple([p for p in namespace.split(".") if p])
    return tuple(namespace)


def list_tables_metadata(cat: Catalog, namespace: str) -> List[Dict[str, Any]]:
    """List tables and lightweight metadata for a namespace.

    Returns a list of dicts with keys: name, last_commit_ts (ISO string or None),
    last_snapshot_id (str or None), total_records (int or None).
    """
    from datetime import datetime, timezone

    from pyiceberg.catalog import load_catalog  # local import for tests

    props = _build_pyiceberg_properties(cat)
    ice_catalog = load_catalog(cat.name, **props)

    ns_tuple = _to_namespace_tuple(namespace)
    idents = ice_catalog.list_tables(ns_tuple)

    results: List[Dict[str, Any]] = []

    def _ident_to_str(ident: Any) -> str:
        # Accepts strings, tuples, or objects with .namespace/.table
        if isinstance(ident, str):
            return ident
        if isinstance(ident, (tuple, list)):
            return ".".join([*ident])
        # Fallback to attributes if available
        ns = getattr(ident, "namespace", None)
        tbl = getattr(ident, "table", None)
        if ns and tbl:
            if isinstance(ns, (tuple, list)):
                return ".".join([*ns, tbl])
            return f"{ns}.{tbl}"
        return str(ident)

    for ident in idents:
        name_str = _ident_to_str(ident)
        try:
            table = ice_catalog.load_table(ident)
        except Exception:
            # If load fails, include minimal info
            results.append(
                {
                    "name": name_str.split(".")[-1],
                    "last_commit_ts": None,
                    "last_snapshot_id": None,
                    "total_records": None,
                }
            )
            continue

        current_id = getattr(table.metadata, "current_snapshot_id", None)
        snap = None
        try:
            for s in getattr(table.metadata, "snapshots", []) or []:
                if getattr(s, "snapshot_id", None) == current_id:
                    snap = s
                    break
        except Exception:
            snap = None

        ts_iso: str | None = None
        total_records: int | None = None
        if snap is not None:
            ts_ms = getattr(snap, "timestamp_ms", None) or getattr(snap, "timestamp_millis", None)
            if isinstance(ts_ms, (int, float)):
                ts_iso = (
                    datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
            summary = getattr(snap, "summary", None) or {}
            tr = summary.get("total-records") if isinstance(summary, dict) else None
            try:
                total_records = int(tr) if tr is not None else None
            except Exception:
                total_records = None

        results.append(
            {
                "name": name_str.split(".")[-1],
                "last_commit_ts": ts_iso,
                "last_snapshot_id": str(current_id) if current_id is not None else None,
                "total_records": total_records,
            }
        )

    results.sort(key=lambda r: r["name"])  # stable order
    return results


def get_table_schema(cat: Catalog, table_name: str) -> Dict[str, Any]:
    """Get table schema information including columns and types."""
    from pyiceberg.catalog import load_catalog  # local import for tests
    
    props = _build_pyiceberg_properties(cat)
    ice_catalog = load_catalog(cat.name, **props)
    
    table = ice_catalog.load_table(table_name)
    schema = table.schema()
    
    columns = []
    for field in schema.fields:
        columns.append({
            "id": field.field_id,
            "name": field.name,
            "type": str(field.field_type),
            "optional": not field.required,
            "comment": getattr(field, "doc", None),
        })
    
    return {
        "table": table_name,
        "schema_id": schema.schema_id,
        "columns": columns,
    }


def sample_table_data(cat: Catalog, table_name: str, limit: int = 10) -> Dict[str, Any]:
    """Sample data from a table."""
    from pyiceberg.catalog import load_catalog  # local import for tests
    
    props = _build_pyiceberg_properties(cat)
    ice_catalog = load_catalog(cat.name, **props)
    
    table = ice_catalog.load_table(table_name)
    
    # Get column names from schema
    schema = table.schema()
    column_names = [field.name for field in schema.fields]
    
    # Scan table data with limit
    try:
        scan = table.scan(limit=limit)
        arrow_table = scan.to_arrow()
        
        # Convert to list of lists for display
        rows = []
        for i in range(len(arrow_table)):
            row = []
            for col_name in column_names:
                value = arrow_table[col_name][i].as_py()
                row.append(value)
            rows.append(row)
            
        return {
            "table": table_name,
            "columns": column_names,
            "rows": rows,
            "limit": limit,
        }
        
    except Exception as e:
        # If scan fails, return empty result
        return {
            "table": table_name,
            "columns": column_names,
            "rows": [],
            "limit": limit,
            "error": str(e),
        }


def get_catalogs_client(config: Any) -> Any:
    """Create a catalogs client from config.
    
    For now, this is a simple passthrough that returns the config object.
    The TUI will use the config directly to access catalog information.
    """
    return config


def describe_table(cat: Catalog, table_name: str) -> Dict[str, Any]:
    """Get detailed table metadata and properties."""
    from datetime import datetime, timezone
    from pyiceberg.catalog import load_catalog  # local import for tests
    
    props = _build_pyiceberg_properties(cat)
    ice_catalog = load_catalog(cat.name, **props)
    
    table = ice_catalog.load_table(table_name)
    metadata = table.metadata
    
    # Get current snapshot info
    current_snapshot_id = getattr(metadata, "current_snapshot_id", None)
    current_snapshot = None
    if current_snapshot_id:
        for snapshot in getattr(metadata, "snapshots", []):
            if getattr(snapshot, "snapshot_id", None) == current_snapshot_id:
                current_snapshot = snapshot
                break
    
    # Build result
    result = {
        "table": table_name,
        "location": getattr(metadata, "location", None),
        "schema_id": getattr(table.schema(), "schema_id", None),
        "current_snapshot_id": str(current_snapshot_id) if current_snapshot_id else None,
        "column_count": len(table.schema().fields),
        "partition_spec_id": getattr(metadata, "default_spec_id", None),
    }
    
    if current_snapshot:
        ts_ms = getattr(current_snapshot, "timestamp_ms", None)
        if ts_ms:
            result["last_updated"] = (
                datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
        
        summary = getattr(current_snapshot, "summary", {}) or {}
        if isinstance(summary, dict):
            result["operation"] = summary.get("operation")
            if "total-records" in summary:
                try:
                    result["total_records"] = int(summary["total-records"])
                except (ValueError, TypeError):
                    pass
    
    # Add table properties if available
    properties = getattr(metadata, "properties", {}) or {}
    if properties:
        result["properties"] = properties
        
    return result


def get_table_snapshots(cat: Catalog, table_name: str) -> List[Dict[str, Any]]:
    """Get snapshot history for a table."""
    from datetime import datetime, timezone
    from pyiceberg.catalog import load_catalog
    
    props = _build_pyiceberg_properties(cat)
    ice_catalog = load_catalog(cat.name, **props)
    
    table = ice_catalog.load_table(table_name)
    metadata = table.metadata
    
    snapshots = []
    for snapshot in getattr(metadata, "snapshots", []):
        snapshot_id = getattr(snapshot, "snapshot_id", None)
        parent_id = getattr(snapshot, "parent_snapshot_id", None)
        ts_ms = getattr(snapshot, "timestamp_ms", None)
        summary = getattr(snapshot, "summary", {}) or {}
        
        # Format timestamp
        timestamp_str = None
        if ts_ms:
            timestamp_str = (
                datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
                .isoformat()
                .replace("+00:00", "Z")
            )
        
        # Get operation from summary
        operation = summary.get("operation", "unknown")
        total_records = summary.get("total-records", "—")
        added_files = summary.get("added-files-size", "—")
        deleted_files = summary.get("removed-files-size", "—")
        
        snapshots.append({
            "snapshot_id": str(snapshot_id) if snapshot_id else "—",
            "parent_id": str(parent_id) if parent_id else "—",
            "timestamp": timestamp_str or "—",
            "operation": operation,
            "total_records": total_records,
            "added_files_size": added_files,
            "deleted_files_size": deleted_files,
            "is_current": snapshot_id == getattr(metadata, "current_snapshot_id", None)
        })
    
    # Sort by timestamp (newest first)
    snapshots.sort(key=lambda s: s["timestamp"], reverse=True)
    return snapshots


def get_table_branches(cat: Catalog, table_name: str) -> List[Dict[str, Any]]:
    """Get branch information for a table."""
    from pyiceberg.catalog import load_catalog
    
    props = _build_pyiceberg_properties(cat)
    ice_catalog = load_catalog(cat.name, **props)
    
    table = ice_catalog.load_table(table_name)
    metadata = table.metadata
    
    branches = []
    refs = getattr(metadata, "refs", {})
    
    for ref_name, ref_info in refs.items():
        ref_type = getattr(ref_info, "type", "unknown")
        if ref_type == "branch":
            snapshot_id = getattr(ref_info, "snapshot_id", None)
            
            # Find the snapshot details
            snapshot = None
            for s in getattr(metadata, "snapshots", []):
                if getattr(s, "snapshot_id", None) == snapshot_id:
                    snapshot = s
                    break
            
            # Get parent snapshot ID from the snapshot
            parent_id = None
            operation = "unknown"
            if snapshot:
                parent_id = getattr(snapshot, "parent_snapshot_id", None)
                summary = getattr(snapshot, "summary", {}) or {}
                operation = summary.get("operation", "unknown")
            
            branches.append({
                "name": ref_name,
                "snapshot_id": str(snapshot_id) if snapshot_id else "—",
                "parent_ref": str(parent_id) if parent_id else "—",
                "action": operation,
                "type": ref_type,
                "is_current": ref_name == "main"  # Assume main is current
            })
    
    # If no explicit branches found, check for main branch
    if not branches:
        current_snapshot_id = getattr(metadata, "current_snapshot_id", None)
        if current_snapshot_id:
            # Find the current snapshot
            current_snapshot = None
            for s in getattr(metadata, "snapshots", []):
                if getattr(s, "snapshot_id", None) == current_snapshot_id:
                    current_snapshot = s
                    break
            
            parent_id = None
            operation = "unknown"
            if current_snapshot:
                parent_id = getattr(current_snapshot, "parent_snapshot_id", None)
                summary = getattr(current_snapshot, "summary", {}) or {}
                operation = summary.get("operation", "unknown")
            
            branches.append({
                "name": "main",
                "snapshot_id": str(current_snapshot_id),
                "parent_ref": str(parent_id) if parent_id else "—",
                "action": operation,
                "type": "branch", 
                "is_current": True
            })
    
    return branches
