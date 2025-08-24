from __future__ import annotations

import json
import os
import pytest
from click.testing import CliRunner

from icectl.cli import cli


@pytest.fixture
def integration_config():
    """Set up config for integration tests."""
    # Use the test config file
    config_path = os.path.join(os.path.dirname(__file__), "../../infra/icectl.config.yaml")
    os.environ["ICECTL_CONFIG"] = os.path.abspath(config_path)
    yield
    # Clean up
    if "ICECTL_CONFIG" in os.environ:
        del os.environ["ICECTL_CONFIG"]


@pytest.mark.integration
def test_catalogs_list_integration_json(integration_config):
    runner = CliRunner()
    res = runner.invoke(cli, ["--json-output", "catalogs", "list"])  # type: ignore[arg-type]
    assert res.exit_code == 0, res.output
    data = json.loads(res.output)
    assert isinstance(data.get("catalogs"), list)
    assert all({"name", "type"}.issubset(c.keys()) for c in data["catalogs"])  # basic shape


@pytest.mark.integration
def test_db_list_integration_table(integration_config):
    runner = CliRunner()
    res = runner.invoke(cli, ["db", "list"])  # type: ignore[arg-type]
    assert res.exit_code == 0, res.output
    # At least the header should be present; content depends on seeded data
    assert "NAMESPACE" in res.output


@pytest.mark.integration
def test_tables_list_integration(integration_config):
    runner = CliRunner()
    res = runner.invoke(cli, ["tables", "list", "--db", "analytics"])  # type: ignore[arg-type]
    assert res.exit_code == 0, res.output
    # With seeder, at least header prints; optionally assert seeded table names once present
    assert "TABLE" in res.output


@pytest.mark.integration
def test_tables_schema_integration(integration_config):
    runner = CliRunner()
    res = runner.invoke(cli, ["tables", "schema", "analytics.events"])  # type: ignore[arg-type]
    assert res.exit_code == 0, res.output
    assert "COLUMN" in res.output
    assert "TYPE" in res.output
    assert "event_id" in res.output
    assert "user_id" in res.output
    assert "ts" in res.output


@pytest.mark.integration  
def test_tables_schema_integration_json(integration_config):
    runner = CliRunner()
    res = runner.invoke(cli, ["--json-output", "tables", "schema", "analytics.events"])  # type: ignore[arg-type]
    assert res.exit_code == 0, res.output
    data = json.loads(res.output)
    assert data["table"] == "analytics.events"
    assert isinstance(data["columns"], list)
    assert len(data["columns"]) == 3
    column_names = [col["name"] for col in data["columns"]]
    assert "event_id" in column_names
    assert "user_id" in column_names
    assert "ts" in column_names


@pytest.mark.integration
def test_tables_sample_integration(integration_config):
    runner = CliRunner()
    res = runner.invoke(cli, ["tables", "sample", "analytics.events", "-n", "2"])  # type: ignore[arg-type]  
    assert res.exit_code == 0, res.output
    # May show "No data found" or actual data, both are valid


@pytest.mark.integration
def test_tables_describe_integration(integration_config):
    runner = CliRunner()
    res = runner.invoke(cli, ["tables", "describe", "analytics.events"])  # type: ignore[arg-type]
    assert res.exit_code == 0, res.output
    assert "PROPERTY" in res.output
    assert "VALUE" in res.output
    assert "analytics.events" in res.output


@pytest.mark.integration
def test_tables_describe_integration_json(integration_config):
    runner = CliRunner()
    res = runner.invoke(cli, ["--json-output", "tables", "describe", "analytics.events"])  # type: ignore[arg-type]
    assert res.exit_code == 0, res.output
    data = json.loads(res.output)
    assert data["table"] == "analytics.events"
    assert "location" in data
    assert "schema_id" in data
    assert "column_count" in data
    assert data["column_count"] == 3
