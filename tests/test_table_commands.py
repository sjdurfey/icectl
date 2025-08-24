from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock, patch

from click.testing import CliRunner
import pytest

from icectl.cli import cli
from icelib.config import Catalog, FSConfig, SampleEngine


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
    warehouse: test/warehouse
    fs:
      endpoint_url: http://localhost:9000
      region: us-east-1
      profile: null
    sample_engine: { type: local }
        """.strip()
    )
    return cfg


@patch('icelib.clients.get_table_schema')
def test_tables_schema_command(mock_schema, tmp_path, monkeypatch):
    """Test tables schema command with mocked client function."""
    cfg = write_config(tmp_path)
    monkeypatch.setenv("ICECTL_CONFIG", str(cfg))
    
    # Mock the schema response
    mock_schema.return_value = {
        "table": "test_table",
        "schema_id": 0,
        "columns": [
            {"id": 1, "name": "col1", "type": "string", "optional": False, "comment": None},
            {"id": 2, "name": "col2", "type": "int", "optional": True, "comment": "test comment"},
        ]
    }
    
    runner = CliRunner()
    res = runner.invoke(cli, ["tables", "schema", "test_table"])
    assert res.exit_code == 0, res.output
    assert "COLUMN" in res.output
    assert "TYPE" in res.output
    assert "col1" in res.output
    assert "col2" in res.output
    assert "string" in res.output
    assert "required" in res.output
    assert "optional" in res.output


@patch('icelib.clients.get_table_schema')
def test_tables_schema_command_json(mock_schema, tmp_path, monkeypatch):
    """Test tables schema command JSON output."""
    cfg = write_config(tmp_path)
    monkeypatch.setenv("ICECTL_CONFIG", str(cfg))
    
    # Mock the schema response
    mock_schema.return_value = {
        "table": "test_table",
        "schema_id": 0,
        "columns": [
            {"id": 1, "name": "col1", "type": "string", "optional": False, "comment": None},
        ]
    }
    
    runner = CliRunner()
    res = runner.invoke(cli, ["--json-output", "tables", "schema", "test_table"])
    assert res.exit_code == 0, res.output
    
    data = json.loads(res.output)
    assert data["table"] == "test_table"
    assert len(data["columns"]) == 1
    assert data["columns"][0]["name"] == "col1"
    assert data["columns"][0]["type"] == "string"


@patch('icelib.clients.sample_table_data')
def test_tables_sample_command(mock_sample, tmp_path, monkeypatch):
    """Test tables sample command with mocked client function."""
    cfg = write_config(tmp_path)
    monkeypatch.setenv("ICECTL_CONFIG", str(cfg))
    
    # Mock the sample response
    mock_sample.return_value = {
        "table": "test_table",
        "columns": ["col1", "col2"],
        "rows": [
            ["value1", 123],
            ["value2", 456],
        ],
        "limit": 10,
    }
    
    runner = CliRunner()
    res = runner.invoke(cli, ["tables", "sample", "test_table", "-n", "2"])
    assert res.exit_code == 0, res.output
    assert "col1" in res.output
    assert "col2" in res.output
    assert "value1" in res.output
    assert "123" in res.output


@patch('icelib.clients.sample_table_data')
def test_tables_sample_command_no_data(mock_sample, tmp_path, monkeypatch):
    """Test tables sample command with no data."""
    cfg = write_config(tmp_path)
    monkeypatch.setenv("ICECTL_CONFIG", str(cfg))
    
    # Mock empty response
    mock_sample.return_value = {
        "table": "test_table",
        "columns": ["col1", "col2"],
        "rows": [],
        "limit": 10,
    }
    
    runner = CliRunner()
    res = runner.invoke(cli, ["tables", "sample", "test_table"])
    assert res.exit_code == 0, res.output
    assert "No data found" in res.output


@patch('icelib.clients.describe_table')
def test_tables_describe_command(mock_describe, tmp_path, monkeypatch):
    """Test tables describe command with mocked client function."""
    cfg = write_config(tmp_path)
    monkeypatch.setenv("ICECTL_CONFIG", str(cfg))
    
    # Mock the describe response
    mock_describe.return_value = {
        "table": "test_table",
        "location": "s3://bucket/path",
        "schema_id": 0,
        "current_snapshot_id": "123456789",
        "column_count": 3,
        "last_updated": "2024-01-15T10:30:00Z",
        "operation": "overwrite",
    }
    
    runner = CliRunner()
    res = runner.invoke(cli, ["tables", "describe", "test_table"])
    assert res.exit_code == 0, res.output
    assert "PROPERTY" in res.output
    assert "VALUE" in res.output
    assert "test_table" in res.output
    assert "s3://bucket/path" in res.output
    assert "123456789" in res.output


@patch('icelib.clients.describe_table')
def test_tables_describe_command_json(mock_describe, tmp_path, monkeypatch):
    """Test tables describe command JSON output."""
    cfg = write_config(tmp_path)
    monkeypatch.setenv("ICECTL_CONFIG", str(cfg))
    
    # Mock the describe response
    mock_describe.return_value = {
        "table": "test_table",
        "location": "s3://bucket/path",
        "schema_id": 0,
        "current_snapshot_id": "123456789",
        "column_count": 3,
    }
    
    runner = CliRunner()
    res = runner.invoke(cli, ["--json-output", "tables", "describe", "test_table"])
    assert res.exit_code == 0, res.output
    
    data = json.loads(res.output)
    assert data["table"] == "test_table" 
    assert data["location"] == "s3://bucket/path"
    assert data["column_count"] == 3
    assert data["current_snapshot_id"] == "123456789"


def test_tables_schema_missing_table(tmp_path, monkeypatch):
    """Test error handling when table is not found."""
    cfg = write_config(tmp_path)
    monkeypatch.setenv("ICECTL_CONFIG", str(cfg))
    
    # Mock exception
    with patch('icelib.clients.get_table_schema', side_effect=Exception("Table not found")):
        runner = CliRunner()
        res = runner.invoke(cli, ["tables", "schema", "nonexistent_table"])
        assert res.exit_code == 2
        assert "Table 'nonexistent_table' not found" in res.output
        assert "icectl tables list" in res.output


def test_tables_sample_missing_table(tmp_path, monkeypatch):
    """Test error handling for sample command when table is not found."""  
    cfg = write_config(tmp_path)
    monkeypatch.setenv("ICECTL_CONFIG", str(cfg))
    
    # Mock exception
    with patch('icelib.clients.sample_table_data', side_effect=Exception("Table not found")):
        runner = CliRunner()
        res = runner.invoke(cli, ["tables", "sample", "nonexistent_table"])
        assert res.exit_code == 2
        assert "Table 'nonexistent_table' not found" in res.output
        assert "icectl tables list" in res.output


def test_tables_describe_missing_table(tmp_path, monkeypatch):
    """Test error handling for describe command when table is not found."""
    cfg = write_config(tmp_path)
    monkeypatch.setenv("ICECTL_CONFIG", str(cfg))
    
    # Mock exception
    with patch('icelib.clients.describe_table', side_effect=Exception("Table not found")):
        runner = CliRunner()
        res = runner.invoke(cli, ["tables", "describe", "nonexistent_table"])
        assert res.exit_code == 2
        assert "Table 'nonexistent_table' not found" in res.output
        assert "icectl tables list" in res.output


def test_error_handling_with_verbose_suggestions(tmp_path, monkeypatch):
    """Test that verbose mode shows troubleshooting suggestions."""
    cfg = write_config(tmp_path)
    monkeypatch.setenv("ICECTL_CONFIG", str(cfg))
    
    # Mock exception
    with patch('icelib.clients.get_table_schema', side_effect=Exception("Table not found")):
        runner = CliRunner()
        res = runner.invoke(cli, ["-v", "tables", "schema", "nonexistent_table"])
        assert res.exit_code == 2
        assert "Troubleshooting suggestions:" in res.output
        assert "List available tables:" in res.output