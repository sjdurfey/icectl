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
