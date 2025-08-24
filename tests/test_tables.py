from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from icectl.cli import cli


def write_config(tmp_path: Path) -> Path:
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        """
version: 1
default_catalog: prod
catalogs:
  prod:
    type: rest
    uri: http://localhost:8181/catalog/
    warehouse: s3://warehouse/
    fs: { }
        """.strip()
    )
    return cfg


def test_tables_list_json(tmp_path, monkeypatch):
    cfg = write_config(tmp_path)
    monkeypatch.setenv("ICECTL_CONFIG", str(cfg))

    monkeypatch.setattr(
        "icelib.clients.list_tables_metadata",
        lambda _cat, ns: [
            {
                "name": "events",
                "last_commit_ts": None,
                "last_snapshot_id": None,
                "total_records": None,
            },
            {
                "name": "sessions",
                "last_commit_ts": "2025-08-20T03:12:44Z",
                "last_snapshot_id": "2b9c1c02-4d1a-4c6d-95d4-1b3b8c7b9d20",
                "total_records": 123,
            },
        ],
    )

    runner = CliRunner()
    res = runner.invoke(cli, ["--json-output", "tables", "list", "--db", "analytics"])  # type: ignore[arg-type]
    assert res.exit_code == 0, res.output
    data = json.loads(res.output)
    assert data["namespace"] == "analytics"
    names = [t["name"] for t in data["tables"]]
    assert names == ["events", "sessions"]


def test_tables_list_table_output(tmp_path, monkeypatch):
    cfg = write_config(tmp_path)
    monkeypatch.setenv("ICECTL_CONFIG", str(cfg))

    monkeypatch.setattr(
        "icelib.clients.list_tables_metadata",
        lambda _cat, ns: [
            {"name": "events", "last_commit_ts": None, "last_snapshot_id": None, "total_records": None}
        ],
    )

    runner = CliRunner()
    res = runner.invoke(cli, ["tables", "list", "--db", "analytics"])  # type: ignore[arg-type]
    assert res.exit_code == 0, res.output
    assert "TABLE" in res.output and "events" in res.output

