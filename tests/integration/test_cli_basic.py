from __future__ import annotations

import json
import os
import pytest
from click.testing import CliRunner

from icectl.cli import cli


@pytest.mark.integration
def test_catalogs_list_integration_json():
    runner = CliRunner()
    res = runner.invoke(cli, ["--json-output", "catalogs", "list"])  # type: ignore[arg-type]
    assert res.exit_code == 0, res.output
    data = json.loads(res.output)
    assert isinstance(data.get("catalogs"), list)
    assert all({"name", "type"}.issubset(c.keys()) for c in data["catalogs"])  # basic shape


@pytest.mark.integration
def test_db_list_integration_table():
    runner = CliRunner()
    res = runner.invoke(cli, ["db", "list"])  # type: ignore[arg-type]
    assert res.exit_code == 0, res.output
    # At least the header should be present; content depends on seeded data
    assert "NAMESPACE" in res.output


@pytest.mark.integration
def test_tables_list_integration():
    runner = CliRunner()
    res = runner.invoke(cli, ["tables", "list", "--db", "analytics"])  # type: ignore[arg-type]
    assert res.exit_code == 0, res.output
    # With seeder, at least header prints; optionally assert seeded table names once present
    assert "TABLE" in res.output
