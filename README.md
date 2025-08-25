# Iceberg Navigator (`icectl`)

A fast, no-frills CLI for navigating Apache Iceberg catalogs. Output is plain and script-friendly; JSON is always pretty-printed. A future TUI will reuse the same core library.

## Quick Start

1. **Prereqs**
   - Python **3.12**
   - **uv** (package/workflow manager): <https://github.com/astral-sh/uv>
   - Docker + Docker Compose (for integration env)

2. **Install deps & hooks**
   ```sh
   make deps        # uv sync
   make hook        # pre-commit with ruff
   ```

3. **Build (no publish)**
   ```sh
   make build       # uv build → dist/ wheel + sdist
   ```

4. **Install onto $PATH (local, no PyPI)**
   - **Option A (recommended): uv tool**
     ```sh
     make install   # uv tool install --force --from . icectl
     icectl --help
     ```
   - **Option B: pipx**
     ```sh
     pipx install .
     icectl --help
     ```

5. **Bring up demo infra (MinIO + Lakekeeper)**
   ```sh
   make infra-reset   # down + up + seed
   ```

6. **Run commands**
   ```sh
   # List available catalogs
   icectl catalogs list
   
   # Show catalog details
   icectl catalogs show --catalog prod
   
   # List namespaces/databases
   icectl db list --catalog prod
   
   # List tables in a namespace
   icectl tables list --catalog prod --db analytics
   
   # Show table schema
   icectl tables schema analytics.events --catalog prod
   
   # Sample table data
   icectl tables sample analytics.events --catalog prod -n 5
   
   # Show detailed table metadata
   icectl tables describe analytics.events --catalog prod
   
   # JSON output for any command
   icectl --json-output tables list --db analytics
   ```

---

## Configuration

`icectl` reads YAML from:
1. `ICECTL_CONFIG` (explicit path override), else
2. `$XDG_CONFIG_HOME/icectl/config.yaml`, else
3. `$XDG_CONFIG_DIRS/icectl/config.yaml`

**Example** (compose-friendly):
```yaml
version: 1
default_catalog: prod

catalogs:
  prod:
    type: rest
    uri: http://localhost:8181/catalog/
    warehouse: s3://warehouse/
    fs:
      endpoint_url: http://localhost:9000   # MinIO endpoint
      region: us-east-1
      profile: null
    sample_engine: { type: local }

    overrides:
      warehouse: s3://warehouse/
      s3.endpoint: http://localhost:9000
      s3.path-style-access: true
      s3.region: us-east-1
      s3.access-key-id: ${MINIO_ROOT_USER}
      s3.secret-access-key: ${MINIO_ROOT_PASSWORD}
```

- Values in `overrides` are passed **as-is** to PyIceberg after env expansion.
- Secrets should live in env vars; human output redacts secret-like keys.

---

## Makefile Targets

Use `make` for repeatable workflows. These targets assume `uv` and Docker are available.

### Tooling & Deps
- `make deps` — Install/sync deps with `uv sync` (creates/updates the venv).
- `make hook` — Install pre-commit hooks (`uv run pre-commit install`).

### Lint & Test
- `make fmt` — Format code (`uv run ruff format`).
- `make lint` — Lint & autofix (`uv run ruff check --fix`).
- `make test` — Unit tests (`uv run pytest -q`).
- `make check` — Convenience: `fmt` + `lint` + `test`.

### Infra
- `make infra-up` — Start MinIO + Lakekeeper (detached) and wait for healthy.
- `make infra-seed` — Seed the warehouse with tiny Parquet tables and commits.
- `make infra-logs` — Tail logs.
- `make infra-down` — Stop and clean containers/volumes.
- `make infra-reset` — `infra-down` + prune + `infra-up` + `infra-seed`.

### Build & Install
- `make build` — Build wheel/sdist with `uv build`.
- `make install` — Install CLI onto `$PATH` using `uv tool install --force --from . icectl`.
- `make uninstall` — Remove installed CLI (`uv tool uninstall icectl`).
- `make dist-clean` — Remove `dist/` artifacts.

### Dev Run
- `make run` — Run `icectl` from repo venv: `uv run icectl $(ARGS)`.
- `make icectl` — Alias for `make run`.

> Tip: `make icectl ARGS="table sample analytics.events --catalog prod -n 5"`

---

## CLI Commands

### Catalog Commands
- `icectl catalogs list` — List all configured catalogs
- `icectl catalogs show [--catalog NAME]` — Show catalog details (uses default if not specified)

### Database/Namespace Commands  
- `icectl db list [--catalog NAME]` — List databases/namespaces in a catalog

### Table Commands
- `icectl tables list --db NAMESPACE [--catalog NAME]` — List tables with metadata
- `icectl tables schema TABLE_NAME [--catalog NAME]` — Show table schema and columns
- `icectl tables sample TABLE_NAME [--catalog NAME] [-n COUNT]` — Sample table data
- `icectl tables describe TABLE_NAME [--catalog NAME]` — Show detailed table metadata

### Global Options
- `--json-output` — Output JSON instead of tables (always pretty-printed)
- `-v, --verbose` — Enable verbose logging and show troubleshooting suggestions on errors
- `-h, --help` — Show help

### Usage Examples

**Basic Navigation:**
```sh
# Explore the data warehouse
icectl catalogs list
icectl db list
icectl tables list --db analytics

# Examine a specific table
icectl tables schema analytics.events  
icectl tables sample analytics.events -n 10
icectl tables describe analytics.events
```

**JSON Output for Scripting:**
```sh
# Get machine-readable output
icectl --json-output catalogs list
icectl --json-output tables schema analytics.events | jq '.columns[].name'
icectl --json-output tables describe analytics.events | jq '.location'
```

**Error Troubleshooting:**
```sh
# Use verbose mode for detailed error help
icectl -v tables schema nonexistent.table
# Shows error context and specific troubleshooting suggestions
```

**Output Conventions**
- Plain table output via `tabulate`
- JSON output is always pretty-printed and sorted
- Iceberg types are printed exactly as defined (no remapping)
- Snapshot IDs are never truncated
- Per-cell truncation defaults to 80 chars (`--max-col-width`, `--no-truncate` to disable)

---

## Installation Notes (No PyPI)

This repo is configured for **local install**:
- `make build` → produces `dist/*.whl` and `dist/*.tar.gz`
- `make install` → installs via **uv tool** to your user PATH
- Alternatively: `pipx install .`

When/if published to a registry, `uvx icectl` will work without cloning.

---

## Troubleshooting

### Common Issues

**Configuration Problems:**
- **Config not found**: Set `ICECTL_CONFIG=/full/path/config.yaml` or create `~/.config/icectl/config.yaml`
- **Wrong warehouse format**: Use project-id/warehouse-name format for Lakekeeper (e.g., `abc123.../prod`)
- **Catalog not found**: Check catalog name spelling in your config file

**Connection Issues:**
- **Services not running**: Run `make infra-up` or `make infra-reset` to start Docker services
- **Connection timeout**: Check that services are healthy with `docker-compose ps` and `make infra-logs`
- **Port conflicts**: Ensure ports 8181 (Lakekeeper) and 9000 (MinIO) are available

**Authentication Issues:**
- **MinIO credentials**: Check `MINIO_ROOT_USER`/`MINIO_ROOT_PASSWORD` or hardcode in config
- **AWS credentials**: Ensure `AWS_PROFILE` or `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` are set
- **S3 permissions**: Verify your credentials have access to the warehouse bucket

**Data Issues:**
- **Table not found**: Use `icectl tables list --db <namespace>` to see available tables
- **Empty results**: Run `make infra-seed` to populate sample data
- **Schema errors**: Table may have incompatible Iceberg schema

### Getting Help

1. **Use verbose mode** for detailed error messages:
   ```sh
   icectl -v tables schema analytics.events
   ```

2. **Check service health**:
   ```sh
   make infra-logs                    # View service logs
   curl http://localhost:8181/health  # Check Lakekeeper
   docker-compose ps                  # Check container status
   ```

3. **Validate configuration**:
   ```sh
   icectl catalogs list              # Test basic config
   icectl catalogs show              # Show detailed catalog config
   ```

4. **Reset everything**:
   ```sh
   make infra-reset                  # Reset Docker services and data
   ```

The CLI provides context-aware error messages with specific suggestions when run with `-v/--verbose` flag.

---

## TUI (Text User Interface)

An interactive terminal-based interface for exploring Iceberg catalogs is planned. See [tui_design.md](tui_design.md) for detailed design specifications.

**Launch TUI:**
```sh
icectl tui
```

**Key TUI Features:**
- **Vim-style navigation**: `:table events`, `:catalog prod`, `:database analytics`
- **Real-time search**: Type to filter catalogs/databases/tables by name
- **Context-aware refresh**: `:r` refreshes current scope (catalogs/databases/tables)
- **Interactive exploration**: Navigate with arrow keys, mouse support
- **Error resilience**: Graceful error handling, never crashes

The TUI reuses the same `icelib` core library as the CLI, ensuring consistent behavior and shared business logic.
