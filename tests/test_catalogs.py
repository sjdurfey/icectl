from __future__ import annotations

import json
import os
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
    fs:
      endpoint_url: http://localhost:9000
      region: us-east-1
      profile: null
    sample_engine: { type: local }
  dev:
    type: hive
    uri: thrift://hms.dev:9083
        """.strip()
    )
    return cfg


def test_catalogs_list_json(tmp_path, monkeypatch):
    cfg = write_config(tmp_path)
    monkeypatch.setenv("ICECTL_CONFIG", str(cfg))

    runner = CliRunner()
    res = runner.invoke(cli, ["--json-output", "catalogs", "list"])  # type: ignore[arg-type]
    assert res.exit_code == 0, res.output
    data = json.loads(res.output)
    names = {c["name"] for c in data["catalogs"]}
    assert names == {"prod", "dev"}


def test_catalogs_show_default(tmp_path, monkeypatch):
    cfg = write_config(tmp_path)
    monkeypatch.setenv("ICECTL_CONFIG", str(cfg))

    runner = CliRunner()
    res = runner.invoke(cli, ["catalogs", "show"])  # table output
    assert res.exit_code == 0, res.output
    assert "name" in res.output and "prod" in res.output

