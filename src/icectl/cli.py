from __future__ import annotations

import json
import logging
from typing import Optional

import click
from tabulate import tabulate

from icelib.config import ConfigError, load_config
import icelib.clients as clients
from icelib.errors import format_error_message, format_config_error, suggest_troubleshooting_steps


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "--json-output",
    "json_output",
    is_flag=True,
    help="Output JSON instead of tables (pretty-printed)",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Enable verbose logging to stderr (what the CLI is doing)",
)
@click.pass_context
def cli(ctx: click.Context, json_output: bool, verbose: bool) -> None:
    """Iceberg Navigator CLI.

    Explore Apache Iceberg catalogs using configuration loaded via XDG or
    the ICECTL_CONFIG environment variable. JSON output is always pretty-printed.
    """
    ctx.ensure_object(dict)
    ctx.obj["json"] = json_output
    ctx.obj["verbose"] = verbose

    # Configure logging once per process
    level = logging.INFO if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    logging.getLogger("pyiceberg").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


@cli.group()
@click.pass_context
def catalogs(ctx: click.Context) -> None:  # noqa: D401
    """Catalog-related commands."""
    # nothing to do here; subcommands below
    pass


@catalogs.command("list")
@click.pass_context
def catalogs_list(ctx: click.Context) -> None:
    """List configured catalogs."""
    log = logging.getLogger("icectl.catalogs")
    try:
        log.info("Loading config...")
        cfg = load_config()
        log.info("Loaded config from %s", getattr(cfg, "source_path", "<unknown>"))
    except ConfigError as e:
        click.echo(format_config_error(e), err=True)
        raise SystemExit(2)

    rows = []
    for name, cat in sorted(cfg.catalogs.items()):
        rows.append(
            [
                name,
                cat.type,
                (cat.uri or "—"),
                "yes" if (cfg.default_catalog == name) else "—",
            ]
        )

    if ctx.obj.get("json"):
        out = {
            "catalogs": [
                {
                    "name": r[0],
                    "type": r[1],
                    "uri": None if r[2] == "—" else r[2],
                    "default": r[3] == "yes",
                }
                for r in rows
            ]
        }
        click.echo(json.dumps(out, indent=2, sort_keys=True))
    else:
        log.info("Rendering table output for %d catalogs", len(rows))
        click.echo(tabulate(rows, headers=["NAME", "TYPE", "URI", "DEFAULT"]))


@catalogs.command("show")
@click.option("--catalog", "catalog_name", help="Catalog name; uses default if omitted")
@click.pass_context
def catalogs_show(ctx: click.Context, catalog_name: Optional[str]) -> None:
    """Show details for a catalog."""
    log = logging.getLogger("icectl.catalogs")
    try:
        log.info("Loading config...")
        cfg = load_config()
        log.info("Loaded config from %s", getattr(cfg, "source_path", "<unknown>"))
    except ConfigError as e:
        click.echo(format_config_error(e), err=True)
        raise SystemExit(2)

    name = catalog_name or cfg.default_catalog
    if not name:
        click.echo("No catalog specified and no default_catalog set in config", err=True)
        raise SystemExit(2)

    cat = cfg.catalogs.get(name)
    if not cat:
        click.echo(f"Catalog not found: {name}", err=True)
        raise SystemExit(2)

    if ctx.obj.get("json"):
        out = {
            "name": cat.name,
            "type": cat.type,
            "uri": cat.uri,
            "warehouse": cat.warehouse,
            "sample_engine": {"type": getattr(cat.sample_engine, "type", "local")},
            "default": cfg.default_catalog == name,
            "fs": {
                "endpoint_url": cat.fs.endpoint_url,
                "region": cat.fs.region,
                "profile": cat.fs.profile,
            },
        }
        if cat.overrides:
            out["overrides"] = cat.overrides
        click.echo(json.dumps(out, indent=2, sort_keys=True))
        return

    rows = [
        ["name", cat.name],
        ["type", cat.type],
        ["uri", cat.uri or "—"],
        ["warehouse", cat.warehouse or "—"],
        ["sample_engine", getattr(cat.sample_engine, "type", "local")],
        ["profile", cat.fs.profile or "—"],
        ["region", cat.fs.region or "—"],
        ["default", "yes" if (cfg.default_catalog == name) else "—"],
    ]
    log.info("Rendering catalog details for '%s'", name)
    click.echo(tabulate(rows, headers=["FIELD", "VALUE"]))


def main() -> None:  # entry point
    cli(standalone_mode=True)


if __name__ == "__main__":  # pragma: no cover
    main()


# DB commands


@cli.group()
@click.pass_context
def db(ctx: click.Context) -> None:  # noqa: D401
    """Database/namespace commands."""
    pass


@db.command("list")
@click.option("--catalog", "catalog_name", help="Catalog name; uses default if omitted")
@click.pass_context
def db_list(ctx: click.Context, catalog_name: Optional[str]) -> None:
    """List namespaces in a catalog."""
    log = logging.getLogger("icectl.db")
    try:
        log.info("Loading config...")
        cfg = load_config()
        log.info("Loaded config from %s", getattr(cfg, "source_path", "<unknown>"))
    except ConfigError as e:
        click.echo(format_config_error(e), err=True)
        raise SystemExit(2)

    name = catalog_name or cfg.default_catalog
    if not name:
        click.echo("No catalog specified and no default_catalog set in config", err=True)
        raise SystemExit(2)

    cat = cfg.catalogs.get(name)
    if not cat:
        click.echo(f"Catalog not found: {name}", err=True)
        raise SystemExit(2)

    try:
        log.info("Listing namespaces from catalog '%s'", name)
        namespaces = clients.list_namespaces(cat)
        log.info("Found %d namespaces", len(namespaces))
    except Exception as e:  # surface helpful error without stack
        error_msg = format_error_message("list namespaces", e, {"catalog": name})
        click.echo(error_msg, err=True)
        if ctx.obj.get("verbose"):
            suggestions = suggest_troubleshooting_steps("list namespaces", e)
            if suggestions:
                click.echo("\nTroubleshooting suggestions:", err=True)
                for suggestion in suggestions[:3]:  # Show top 3 suggestions
                    click.echo(f"  • {suggestion}", err=True)
        raise SystemExit(2)

    if ctx.obj.get("json"):
        out = {"namespaces": namespaces}
        click.echo(json.dumps(out, indent=2, sort_keys=True))
        return

    if not namespaces:
        click.echo("No namespaces found")
        return

    rows = [[ns] for ns in namespaces]
    log.info("Rendering %d namespaces", len(rows))
    click.echo(tabulate(rows, headers=["NAMESPACE"]))


# TABLES commands


@cli.group()
@click.pass_context
def tables(ctx: click.Context) -> None:  # noqa: D401
    """Table-related commands."""
    pass


@tables.command("list")
@click.option("--catalog", "catalog_name", help="Catalog name; uses default if omitted")
@click.option("--db", "namespace", required=True, help="Namespace (e.g., analytics or a.b)")
@click.pass_context
def tables_list(ctx: click.Context, catalog_name: Optional[str], namespace: str) -> None:
    """List tables in a namespace with basic metadata."""
    log = logging.getLogger("icectl.tables")
    try:
        log.info("Loading config...")
        cfg = load_config()
        log.info("Loaded config from %s", getattr(cfg, "source_path", "<unknown>"))
    except ConfigError as e:
        click.echo(format_config_error(e), err=True)
        raise SystemExit(2)

    name = catalog_name or cfg.default_catalog
    if not name:
        click.echo("No catalog specified and no default_catalog set in config", err=True)
        raise SystemExit(2)

    cat = cfg.catalogs.get(name)
    if not cat:
        click.echo(f"Catalog not found: {name}", err=True)
        raise SystemExit(2)

    try:
        log.info("Listing tables for namespace '%s' in catalog '%s'", namespace, name)
        items = clients.list_tables_metadata(cat, namespace)
        log.info("Found %d tables", len(items))
    except Exception as e:  # surface helpful error without stack
        error_msg = format_error_message("list tables", e, {"namespace": namespace, "catalog": name})
        click.echo(error_msg, err=True)
        if ctx.obj.get("verbose"):
            suggestions = suggest_troubleshooting_steps("list tables", e)
            if suggestions:
                click.echo("\nTroubleshooting suggestions:", err=True)
                for suggestion in suggestions[:3]:  # Show top 3 suggestions
                    click.echo(f"  • {suggestion}", err=True)
        raise SystemExit(2)

    if ctx.obj.get("json"):
        out = {"namespace": namespace, "tables": items}
        click.echo(json.dumps(out, indent=2, sort_keys=True))
        return

    if not items:
        click.echo("No tables found")
        return

    rows = []
    for it in items:
        rows.append(
            [
                it.get("name"),
                it.get("last_commit_ts") or "—",
                it.get("last_snapshot_id") or "—",
                it.get("total_records") if it.get("total_records") is not None else "—",
            ]
        )
    log.info("Rendering %d tables", len(rows))
    click.echo(
        tabulate(
            rows,
            headers=["TABLE", "LAST_COMMIT_TS", "LAST_SNAPSHOT_ID", "TOTAL_RECORDS"],
        )
    )


@tables.command("schema")
@click.argument("table_name")
@click.option("--catalog", "catalog_name", help="Catalog name; uses default if omitted")
@click.pass_context
def table_schema(ctx: click.Context, table_name: str, catalog_name: Optional[str]) -> None:
    """Show table schema and columns."""
    log = logging.getLogger("icectl.table")
    try:
        log.info("Loading config...")
        cfg = load_config()
        log.info("Loaded config from %s", getattr(cfg, "source_path", "<unknown>"))
    except ConfigError as e:
        click.echo(format_config_error(e), err=True)
        raise SystemExit(2)

    name = catalog_name or cfg.default_catalog
    if not name:
        click.echo("No catalog specified and no default_catalog set in config", err=True)
        raise SystemExit(2)

    cat = cfg.catalogs.get(name)
    if not cat:
        click.echo(f"Catalog not found: {name}", err=True)
        raise SystemExit(2)

    try:
        log.info("Getting schema for table '%s' in catalog '%s'", table_name, name)
        schema_info = clients.get_table_schema(cat, table_name)
        log.info("Found schema with %d columns", len(schema_info))
    except Exception as e:
        error_msg = format_error_message("get table schema", e, {"table": table_name, "catalog": name})
        click.echo(error_msg, err=True)
        if ctx.obj.get("verbose"):
            suggestions = suggest_troubleshooting_steps("get table schema", e)
            if suggestions:
                click.echo("\nTroubleshooting suggestions:", err=True)
                for suggestion in suggestions[:3]:  # Show top 3 suggestions
                    click.echo(f"  • {suggestion}", err=True)
        raise SystemExit(2)

    if ctx.obj.get("json"):
        click.echo(json.dumps(schema_info, indent=2, sort_keys=True))
        return

    if not schema_info.get("columns"):
        click.echo("No columns found")
        return

    rows = []
    for col in schema_info["columns"]:
        rows.append([
            col.get("name", ""),
            col.get("type", ""),
            "optional" if col.get("optional", True) else "required",
            col.get("comment", "—"),
        ])
    
    log.info("Rendering schema for table '%s'", table_name)
    click.echo(tabulate(rows, headers=["COLUMN", "TYPE", "NULLABLE", "COMMENT"]))


@tables.command("sample")
@click.argument("table_name")
@click.option("--catalog", "catalog_name", help="Catalog name; uses default if omitted")
@click.option("-n", "--limit", default=10, help="Number of rows to sample (default: 10)")
@click.option("--max-col-width", default=80, help="Max column width for display (default: 80)")
@click.option("--no-truncate", is_flag=True, help="Don't truncate long column values")
@click.pass_context
def table_sample(
    ctx: click.Context,
    table_name: str, 
    catalog_name: Optional[str],
    limit: int,
    max_col_width: int,
    no_truncate: bool
) -> None:
    """Sample data from a table."""
    log = logging.getLogger("icectl.table")
    try:
        log.info("Loading config...")
        cfg = load_config()
        log.info("Loaded config from %s", getattr(cfg, "source_path", "<unknown>"))
    except ConfigError as e:
        click.echo(format_config_error(e), err=True)
        raise SystemExit(2)

    name = catalog_name or cfg.default_catalog
    if not name:
        click.echo("No catalog specified and no default_catalog set in config", err=True)
        raise SystemExit(2)

    cat = cfg.catalogs.get(name)
    if not cat:
        click.echo(f"Catalog not found: {name}", err=True)
        raise SystemExit(2)

    try:
        log.info("Sampling %d rows from table '%s' in catalog '%s'", limit, table_name, name)
        sample_data = clients.sample_table_data(cat, table_name, limit)
        log.info("Retrieved %d sample rows", len(sample_data.get("rows", [])))
    except Exception as e:
        error_msg = format_error_message("sample table data", e, {"table": table_name, "catalog": name})
        click.echo(error_msg, err=True)
        if ctx.obj.get("verbose"):
            suggestions = suggest_troubleshooting_steps("sample table data", e)
            if suggestions:
                click.echo("\nTroubleshooting suggestions:", err=True)
                for suggestion in suggestions[:3]:  # Show top 3 suggestions
                    click.echo(f"  • {suggestion}", err=True)
        raise SystemExit(2)

    if ctx.obj.get("json"):
        click.echo(json.dumps(sample_data, indent=2, sort_keys=True))
        return

    rows = sample_data.get("rows", [])
    if not rows:
        click.echo("No data found")
        return

    # Truncate column values if needed
    if not no_truncate and max_col_width > 0:
        truncated_rows = []
        for row in rows:
            truncated_row = []
            for cell in row:
                cell_str = str(cell) if cell is not None else ""
                if len(cell_str) > max_col_width:
                    cell_str = cell_str[:max_col_width-3] + "..."
                truncated_row.append(cell_str)
            truncated_rows.append(truncated_row)
        rows = truncated_rows

    headers = sample_data.get("columns", [])
    log.info("Rendering %d sample rows", len(rows))
    click.echo(tabulate(rows, headers=headers))


@tables.command("describe")  
@click.argument("table_name")
@click.option("--catalog", "catalog_name", help="Catalog name; uses default if omitted")
@click.pass_context
def table_describe(ctx: click.Context, table_name: str, catalog_name: Optional[str]) -> None:
    """Show detailed table metadata and properties."""
    log = logging.getLogger("icectl.table")
    try:
        log.info("Loading config...")
        cfg = load_config()
        log.info("Loaded config from %s", getattr(cfg, "source_path", "<unknown>"))
    except ConfigError as e:
        click.echo(format_config_error(e), err=True)
        raise SystemExit(2)

    name = catalog_name or cfg.default_catalog
    if not name:
        click.echo("No catalog specified and no default_catalog set in config", err=True)
        raise SystemExit(2)

    cat = cfg.catalogs.get(name)
    if not cat:
        click.echo(f"Catalog not found: {name}", err=True)
        raise SystemExit(2)

    try:
        log.info("Describing table '%s' in catalog '%s'", table_name, name)
        table_info = clients.describe_table(cat, table_name)
        log.info("Retrieved table metadata")
    except Exception as e:
        error_msg = format_error_message("describe table", e, {"table": table_name, "catalog": name})
        click.echo(error_msg, err=True)
        if ctx.obj.get("verbose"):
            suggestions = suggest_troubleshooting_steps("describe table", e)
            if suggestions:
                click.echo("\nTroubleshooting suggestions:", err=True)
                for suggestion in suggestions[:3]:  # Show top 3 suggestions
                    click.echo(f"  • {suggestion}", err=True)
        raise SystemExit(2)

    if ctx.obj.get("json"):
        click.echo(json.dumps(table_info, indent=2, sort_keys=True))
        return

    # Display table information as key-value pairs
    rows = []
    for key, value in table_info.items():
        if isinstance(value, (dict, list)):
            # Skip complex objects for table view
            continue
        rows.append([key, str(value) if value is not None else "—"])
    
    log.info("Rendering table description")
    click.echo(tabulate(rows, headers=["PROPERTY", "VALUE"]))


@cli.command()
@click.pass_context
def tui(ctx: click.Context) -> None:
    """Launch interactive TUI for catalog exploration."""
    try:
        from icetui.app import run_tui
        run_tui()
    except ImportError as e:
        click.echo(f"TUI dependencies not available: {e}", err=True)
        raise SystemExit(1)
    except Exception as e:
        error_msg = format_error_message("launch TUI", e, {})
        click.echo(error_msg, err=True)
        raise SystemExit(1)
