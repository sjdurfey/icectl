from __future__ import annotations

import json
from pathlib import Path
from typing import List

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


def test_db_list_json(tmp_path, monkeypatch):
    cfg = write_config(tmp_path)
    monkeypatch.setenv("ICECTL_CONFIG", str(cfg))

    # Avoid real pyiceberg calls; mock our wrapper
    def fake_list_namespaces(_cat) -> List[str]:
        return ["analytics", "raw", "staging.events"]

    monkeypatch.setattr("icelib.clients.list_namespaces", fake_list_namespaces)

    runner = CliRunner()
    res = runner.invoke(cli, ["--json-output", "db", "list"])  # type: ignore[arg-type]
    assert res.exit_code == 0, res.output
    data = json.loads(res.output)
    assert data["namespaces"] == ["analytics", "raw", "staging.events"]


def test_db_list_table(tmp_path, monkeypatch):
    cfg = write_config(tmp_path)
    monkeypatch.setenv("ICECTL_CONFIG", str(cfg))

    monkeypatch.setattr(
        "icelib.clients.list_namespaces",
        lambda _cat: ["analytics", "raw"],
    )

    runner = CliRunner()
    res = runner.invoke(cli, ["db", "list"])  # type: ignore[arg-type]
    assert res.exit_code == 0, res.output
    assert "NAMESPACE" in res.output
    assert "analytics" in res.output and "raw" in res.output

