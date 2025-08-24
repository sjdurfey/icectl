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
   icectl catalogs list
   icectl db list --catalog prod
   icectl tables list --catalog prod --db analytics
   icectl table schema analytics.events --catalog prod
   icectl table sample analytics.events --catalog prod -n 5
   icectl table describe analytics.events --catalog prod
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

## CLI Surfaces

- `catalogs list`, `catalogs show`
- `db list`
- `tables list`
- `table schema`
- `table sample`
- `table describe`

**Conventions**
- Plain table output via `tabulate`.
- JSON output is **always pretty-printed**.
- Types are printed **exactly** as defined in Iceberg (no remapping).
- Snapshot IDs are **never truncated**.
- Nested columns use dot paths (`attrs.ip`).
- Per-cell truncation defaults to 80 chars in table output (`--max-col-width 80`, `--no-truncate` to disable).

---

## Installation Notes (No PyPI)

This repo is configured for **local install**:
- `make build` → produces `dist/*.whl` and `dist/*.tar.gz`
- `make install` → installs via **uv tool** to your user PATH
- Alternatively: `pipx install .`

When/if published to a registry, `uvx icectl` will work without cloning.

---

## Troubleshooting

- **Auth to MinIO/AWS**: ensure your env has the right credentials (`MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, or `AWS_PROFILE` / `AWS_*` env vars).
- **Config not found**: set `ICECTL_CONFIG=/full/path/config.yaml`.
- **Compose not healthy**: `make infra-logs` and inspect services.
- **Pre-commit blocking**: run `make fmt && make lint` to fix formatting/lint issues.

---

## Roadmap

After the CLI MVP, we’ll spec a TUI that calls the same `icelib` APIs, keeping all business logic shared.
