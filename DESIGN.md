# Iceberg Navigator (`icectl`) — Design Doc (CLI MVP → TUI later)

> **Status:** Locked plan (design-only; **no code**).  
> **Goal:** Ship a fast, no-frills CLI for navigating Apache Iceberg catalogs. The TUI will reuse the same business logic after the CLI MVP lands.

---

## 1) Objective

Build a fast, script-friendly Python CLI for navigating Apache Iceberg catalogs using **Click** + **PyIceberg**, with reusable business logic in a core package that the later TUI will call directly.

- Python **3.12**
- Package/workflow manager: **uv** (use `uv add`, `uv sync`, `uv run`, `uv build`, `uv tool install`, `uvx`)
- Lint/format: **ruff** (pre-commit hook; blocks commits on failure)
- Tests: **pytest**
- Output: **plain tables via tabulate**; **JSON always pretty-printed**

---

## 2) Scope (MVP = CLI)

**In scope (v1 CLI)**

- Multiple catalog support via **YAML** config following **XDG** paths.
- Commands:
  - `catalogs list`, `catalogs show`
  - `db list`
  - `tables list`
  - `table schema`
  - `table sample`
  - `table describe` (schema + last 5 snapshots + basic metadata + file size distribution)
- **Sampling** via local **PyIceberg + PyArrow** only (no pandas).
- **docker-compose**: Lakekeeper + MinIO + a seeding container for deterministic tests.
- **Distribution wiring only** (no publishing): entry points for `pipx`/`uv tool install` usage.

**Out of scope (for MVP)**

- Mutations (create/drop/alter).
- Complex expressions / `--where` (punted for now).
- Secret managers and custom auth brokers (PoC relies on default AWS creds chain or MinIO env).
- TUI implementation (will be spec’d right after the CLI MVP).

---

## 3) Non-negotiables / Locked Decisions

- **Rendering:** *Plain* `tabulate` only (no styles). The CLI is for quick, scriptable tasks; the TUI will handle visual polish.
- **Types:** Print **exact Iceberg types from the table**—no readability mapping.
- **Truncation:** Per-cell truncation only (default `--max-col-width 80`); `--no-truncate` disables truncation. **JSON never truncates**.
- **Snapshot IDs:** Never truncated in any command.
- **Nested columns:** Use dot paths (e.g., `attrs.ip`).
- **Arrow only:** Use `pyarrow` tables/record batches everywhere; **never import pandas** (unless explicitly requested later).
- **Sampler (v1):** **Local** only — PyIceberg to plan + PyArrow to read; supports AWS creds via Arrow’s default provider chain (env, shared file, IMDS).
- **JSON output:** **Always pretty-printed** with stable key order.
- **uv for everything:** use `uv add`, `uv sync`, `uv run`, `uv build`, `uv tool install`. Avoid direct `python`/`pip` in docs and Makefile.
- **After each task:** all tests pass and changes are committed. Each milestone aggregates such commits and is “Done” only when all tasks are merged and green.

---

## 4) CLI Surface (Finalized Outputs)

### 4.1 `icectl catalogs list`

**Table**
```
NAME    TYPE   URI                                  DEFAULT
prod    rest   https://iceberg-catalog.company.com  yes
dev     hive   thrift://hms.dev:9083                —
```

**JSON (pretty)**
```json
{
  "catalogs": [
    {
      "name": "prod",
      "type": "rest",
      "uri": "https://iceberg-catalog.company.com",
      "default": true
    },
    {
      "name": "dev",
      "type": "hive",
      "uri": "thrift://hms.dev:9083",
      "default": false
    }
  ]
}
```

---

### 4.2 `icectl catalogs show [--catalog <name>]`

**Table**
```
FIELD            VALUE
name             prod
type             rest
uri              https://iceberg-catalog.company.com
warehouse        s3://prod-warehouse/
sample_engine    local
profile          —
region           us-east-1
default          yes
```

**JSON (pretty)**
```json
{
  "name": "prod",
  "type": "rest",
  "uri": "https://iceberg-catalog.company.com",
  "warehouse": "s3://prod-warehouse/",
  "sample_engine": { "type": "local" },
  "default": true,
  "fs": { "profile": null, "region": "us-east-1" }
}
```

> **Secrets redaction policy**: in human output (and optionally JSON), known secret-like keys (`*access-key*`, `*secret*`, `*token*`) are redacted; non-sensitive overrides are visible. See “Catalog Overrides” below.

---

### 4.3 `icectl db list [--catalog <name>]`

**Table**
```
NAMESPACE
analytics
raw
staging.events
```

**JSON (pretty)**
```json
{
  "namespaces": [
    "analytics",
    "raw",
    "staging.events"
  ]
}
```

---

### 4.4 `icectl tables list [--catalog <name>] [--db <namespace>]`

> Pure metadata; **snapshot IDs never truncated**.

**Table**
```
TABLE              LAST_COMMIT_TS              LAST_SNAPSHOT_ID                                  TOTAL_RECORDS
events             2025-08-22T19:31:05Z        6f4a3c5a-2b3e-4f7b-9a5a-2e4e7ea0a3f8               128934567
sessions           2025-08-20T03:12:44Z        2b9c1c02-4d1a-4c6d-95d4-1b3b8c7b9d20               —
```

**JSON (pretty)**
```json
{
  "namespace": "analytics",
  "tables": [
    {
      "name": "events",
      "last_commit_ts": "2025-08-22T19:31:05Z",
      "last_snapshot_id": "6f4a3c5a-2b3e-4f7b-9a5a-2e4e7ea0a3f8",
      "total_records": 128934567
    },
    {
      "name": "sessions",
      "last_commit_ts": "2025-08-20T03:12:44Z",
      "last_snapshot_id": "2b9c1c02-4d1a-4c6d-95d4-1b3b8c7b9d20",
      "total_records": null
    }
  ]
}
```

**Mapping**:  
`LAST_COMMIT_TS` = current snapshot `timestampMillis`; `LAST_SNAPSHOT_ID` = `currentSnapshotId`; `TOTAL_RECORDS` = `snapshot.summary["total-records"]` (nullable).

---

### 4.5 `icectl table schema <table> [--catalog ...] [--db ...]`

> Types printed **exactly** as defined in the table (no readability mapping).

**Table**
```
#   COLUMN         TYPE                          REQUIRED   PARTITION_KEY   DOC
1   event_id       string                        yes        —               unique event id
2   user_id        long                          yes        —               —
3   event_type     string                        no         —               —
4   ts             timestamp                     yes        ts              event time
5   attrs          struct<ip:string,ua:string>   no         —               flattened on ingest
```

**JSON (pretty)**
```json
{
  "table": "analytics.events",
  "schema": {
    "fields": [
      { "id": 1, "name": "event_id", "type": "string", "required": true,  "doc": "unique event id", "partition_key": false },
      { "id": 2, "name": "user_id",  "type": "long",   "required": true,  "doc": null,              "partition_key": false },
      { "id": 3, "name": "event_type","type": "string","required": false, "doc": null,              "partition_key": false },
      { "id": 4, "name": "ts",       "type": "timestamp", "required": true, "doc": "event time",    "partition_key": true },
      { "id": 5, "name": "attrs",    "type": "struct<ip:string,ua:string>", "required": false, "doc": "flattened on ingest", "partition_key": false }
    ],
    "partition_spec": [
      { "source": "ts", "transform": "day", "field_id": 1000 }
    ],
    "schema_id": 0
  }
}
```

---

### 4.6 `icectl table sample <table> [-n/--limit N] [--columns ...] [--output ...]`

> Default `--limit 10`; projection supports dot paths; per-cell truncation applies only to table mode. JSON is always pretty-printed and never truncated.

**Table**
```
TABLE: analytics.events    SNAPSHOT: 6f4a3c5a-2b3e-4f7b-9a5a-2e4e7ea0a3f8    COMMIT: 2025-08-22T19:31:05Z    ROWS: 10

event_id     user_id     event_type   ts                      attrs.ip
e_0001       42          view         2025-08-21T15:01:02Z    203.0.113.8
...
```

**JSON (pretty)**
```json
{
  "table": "analytics.events",
  "snapshot_id": "6f4a3c5a-2b3e-4f7b-9a5a-2e4e7ea0a3f8",
  "commit_ts": "2025-08-22T19:31:05Z",
  "limit": 10,
  "columns": ["event_id", "user_id", "event_type", "ts", "attrs.ip"],
  "rows": [
    {
      "event_id": "e_0001",
      "user_id": 42,
      "event_type": "view",
      "ts": "2025-08-21T15:01:02Z",
      "attrs.ip": "203.0.113.8"
    }
  ]
}
```

---

### 4.7 `icectl table describe <table> [--catalog ...] [--db ...] [--size-bins ...]`

> One-stop: schema, last 5 snapshots, basic metadata, and size distribution (from manifests; metadata-only reads).

**Table (sectioned)**

```
TABLE: analytics.events

[METADATA]
location                 s3://warehouse/analytics/events/
format_version           2
current_snapshot_id      6f4a3c5a-2b3e-4f7b-9a5a-2e4e7ea0a3f8
current_commit_ts        2025-08-22T19:31:05Z
last_sequence_number     17
partition_spec           ts: day
total_records            128934567
total_data_files         142
total_delete_files       3
avg_file_size_bytes      7340032
min_file_size_bytes      262144
max_file_size_bytes      16777216

[SCHEMA]
#   COLUMN         TYPE                          REQUIRED   PARTITION_KEY   DOC
...

[SNAPSHOTS] (most recent first, max 5)
COMMIT_TS                 SNAPSHOT_ID                                   OPERATION      TOTAL_RECORDS  ADDED_FILES  REMOVED_FILES
2025-08-22T19:31:05Z      6f4a3c5a-2b3e-4f7b-9a5a-2e4e7ea0a3f8           append         128934567      12           0
...

[FILE SIZE DISTRIBUTION] (current snapshot)
RANGE            COUNT
0-128KB          3
128KB-1MB        18
1MB-8MB          101
8MB+             23
```

**JSON (pretty)**
```json
{
  "table": "analytics.events",
  "metadata": {
    "location": "s3://warehouse/analytics/events/",
    "format_version": 2,
    "current_snapshot_id": "6f4a3c5a-2b3e-4f7b-9a5a-2e4e7ea0a3f8",
    "current_commit_ts": "2025-08-22T19:31:05Z",
    "last_sequence_number": 17,
    "partition_spec": [
      { "source": "ts", "transform": "day", "field_id": 1000 }
    ],
    "total_records": 128934567,
    "total_data_files": 142,
    "total_delete_files": 3,
    "avg_file_size_bytes": 7340032,
    "min_file_size_bytes": 262144,
    "max_file_size_bytes": 16777216
  },
  "schema": { /* same as table schema */ },
  "snapshots": [
    {
      "commit_ts": "2025-08-22T19:31:05Z",
      "snapshot_id": "6f4a3c5a-2b3e-4f7b-9a5a-2e4e7ea0a3f8",
      "operation": "append",
      "summary": {
        "total-records": 128934567,
        "added-data-files": 12,
        "removed-data-files": 0
      }
    }
  ],
  "file_size_distribution": {
    "bins": ["0-128KB", "128KB-1MB", "1MB-8MB", "8MB+"],
    "counts": [3, 18, 101, 23]
  }
}
```

**Default demo bins:** `0-128KB`, `128KB-1MB`, `1MB-8MB`, `8MB+`.  
**Override:** `--size-bins 10MB,100MB,1000MB` (units KB/MB/GB supported).

---

## 5) Configuration (YAML) + Catalog-Specific Overrides

**Search order:** `ICECTL_CONFIG` → `$XDG_CONFIG_HOME/icectl/config.yaml` → `$XDG_CONFIG_DIRS/icectl/config.yaml`

**Example:**
```yaml
version: 1
default_catalog: prod

catalogs:
  prod:
    type: rest
    uri: http://localhost:8181/catalog/
    warehouse: s3://warehouse/
    fs:
      endpoint_url: http://localhost:9000   # MinIO in compose
      region: us-east-1
      profile: null
    sample_engine: { type: local }

    # Catalog-specific overrides passed verbatim to PyIceberg (after env expansion)
    overrides:
      warehouse: s3://warehouse/
      s3.endpoint: http://localhost:9000
      s3.path-style-access: true
      s3.region: us-east-1
      s3.access-key-id: ${MINIO_ROOT_USER}
      s3.secret-access-key: ${MINIO_ROOT_PASSWORD}
```

### Overrides — Rules & Precedence

- **Pass-through:** Keys under `overrides` are forwarded **as-is** to `pyiceberg.catalog.load_catalog(...)` after `${ENV}` expansion.
- **Env expansion:** `${VAR}` is expanded first; if unset, validation fails with a helpful path (e.g., `catalogs.prod.overrides.s3.secret-access-key`).
- **Unknown keys:** Allowed; PyIceberg decides if they’re meaningful.
- **Secrets:** Prefer environment variables; display is redacted in human output.
- **Precedence for PyIceberg config:** base derived fields → **`overrides`** → PyIceberg’s own env handling (if any).
- **Precedence for PyArrow FS (sampling):** env → `fs.*` hints → selected keys from `overrides` (endpoint/region/ak/secret/path-style) → defaults.

---

## 6) Test & Demo Environment (docker-compose)

**Services**
- **MinIO** (S3-compatible): bucket `warehouse` (data).
- **Lakekeeper** (REST catalog): configured to point at MinIO; **warehouse created** via API:
  - Creating a warehouse: <https://docs.lakekeeper.io/getting-started/#creating-a-warehouse>
  - Lakekeeper compose reference: <https://github.com/lakekeeper/lakekeeper/blob/main/docker-compose/docker-compose.yaml>
- **Seeder** (one-shot): uploads tiny Parquet files, registers namespaces/tables, performs commits so snapshot summaries include `total-records`, and size histogram bins have meaningful counts.

**Integration-test expectation:** after `make infra-reset`, all CLI commands succeed against `prod` pointing at Lakekeeper/MinIO.

---

## 7) Distribution Wiring (no publish)

Goal: enable `icectl` to be installed onto `$PATH` **without cloning**, but **do not publish** to PyPI yet.

- **Entry point**: `console_scripts` in `pyproject.toml`
  ```toml
  [project.scripts]
  icectl = "icectl.main:cli"
  ```
- **Build artifacts**: `uv build` → wheel + sdist in `dist/`.
- **Local install options (no registry):**
  - **uv tool (preferred for dev):**
    - `uv tool install --force --from . icectl`  → adds `icectl` to tool bin on `$PATH`
  - **pipx (friendly alternative):**
    - `pipx install .`  or `pipx install dist/<wheel-file>.whl`
- **uvx**: once a registry/VCS source exists, `uvx icectl` (not used until then).

> All dependency work must use **`uv add`** (e.g., `uv add click pyiceberg pyarrow tabulate platformdirs pyyaml pydantic pytest ruff pre-commit`).

---

## 8) Makefile (repeatable tasks)

All common commands are wrapped in `make` targets so contributors don’t have to memorize long invocations. The **README must document** each target, example usage, and prerequisites.

**Tooling & deps**
- `make deps` — `uv sync` (create/update venv); sanity check versions.
- `make hook` — install pre-commit hook via `uv run pre-commit install`.

**Lint & test**
- `make fmt` — `uv run ruff format`
- `make lint` — `uv run ruff check --fix`
- `make test` — `uv run pytest -q`
- `make check` — `make fmt && make lint && make test`

**Infra (compose)**
- `make infra-up` — start MinIO + Lakekeeper (detached), wait for healthy.
- `make infra-seed` — run one-shot seeder to populate warehouse.
- `make infra-logs` — tail logs for all services.
- `make infra-down` — stop and remove containers/volumes.
- `make infra-reset` — `infra-down` + prune + `infra-up` + `infra-seed`.

**Build & install**
- `make build` — `uv build` (wheel + sdist).
- `make install` — local install for PATH:
  - **Option A (uv tool)**: `uv tool install --force --from . icectl`
  - **Option B (pipx)**: `pipx install .`
- `make uninstall` — remove installed tool:
  - **uv tool**: `uv tool uninstall icectl`
  - **pipx**: `pipx uninstall icectl`
- `make dist-clean` — clean `dist/` and build artifacts.

**Dev run helpers**
- `make run` — run `icectl` from the repo venv: `uv run icectl $(ARGS)`
- `make icectl` — alias of `make run`

---

## 9) Tooling & Quality Gates

- **Pre-commit hook** runs `ruff format` and `ruff check` before every commit; commits are blocked on failure.
- **CI (optional)** runs `make check` and compose-based integration tests (opt-in).
- **Type hints** required across the codebase (pyright-compatible).
- **Exit codes**: `0` success; `2` user/config error; `3` catalog error; `4` sampling/read error.

---

## 10) Milestones, Tasks, and “Done” Criteria

> After **each small task**, tests pass and changes are committed. A milestone is **Done** when all its tasks are merged and green.

### Milestone 0 — Repo & Tooling Skeleton
**Tasks**
1. Repo layout (`src/icelib`, `src/icectl`, `tests/`, `docker/`).
2. `pyproject.toml` with Python 3.12, deps added via `uv add`, and `console_scripts` for `icectl`.
3. `uv sync` and lock file committed.
4. `ruff` config + `pre-commit` config; `make deps` & `make hook`.
5. `pytest` config and a placeholder test.
6. Add `DESIGN.md` (this file) + `README.md` (initial; documents Makefile).

**Done**
- `make check` is green.
- Pre-commit blocks bad code locally.
- `make build` (`uv build`) succeeds (wiring only; no publish).

---

### Milestone 1 — Config & XDG Resolution (with Overrides)
**Tasks**
1. YAML schema + env expansion (including `overrides.*` keys).
2. XDG resolution + `ICECTL_CONFIG` override.
3. Validation with helpful path errors for missing/unset envs in overrides.
4. Redaction policy for secret-like keys in human output (and optionally JSON).
5. Unit tests for happy/edge cases (env expansion, redaction).

**Done**
- Invalid configs fail fast with actionable messages.
- 100% coverage for config module.

---

### Milestone 2 — Catalogs Commands
**Tasks**
1. `catalogs list` (config-only).
2. `catalogs show` (non-secret fields; fs hints; redactions as needed).
3. Pretty JSON for both.
4. CLI rendering tests (table + JSON).

**Done**
- Output matches spec; headers/keys are constants.

---

### Milestone 3 — Namespaces & Tables Metadata
**Tasks**
1. Merge base config + `overrides` and pass-through to `pyiceberg.catalog.load_catalog(...)`.
2. `db list` via PyIceberg.
3. `tables list` via metadata only: current snapshot ts, id (not truncated), `total-records` if present.
4. Tests (mocks + later integration).

**Done**
- Alphabetical ordering; ISO timestamps; full snapshot IDs.
- Unit test verifies merged conf equals expected dict (including overrides).

---

### Milestone 4 — Table Schema
**Tasks**
1. `table schema` with exact Iceberg types.
2. Partition keys marked from current spec.
3. Pretty JSON + plain table.
4. Tests for nested types, required flags, partition marking.

**Done**
- Types are unmodified; outputs match spec.

---

### Milestone 5 — Local Sampler (Arrow only)
**Tasks**
1. Plan with PyIceberg; read minimal rows with PyArrow (no filters).
2. `--limit` and `--columns` (dot paths).
3. Header includes FQN, snapshot ID, commit ts, returned rows.
4. Table truncation behaviors; JSON pretty.
5. Arrow FS precedence: env → `fs` → selected `overrides` → defaults.
6. Unit + integration tests.

**Done**
- Exactly N rows (unless fewer exist); no pandas imports; full snapshot IDs.

---

### Milestone 6 — `table describe`
**Tasks**
1. Metadata section (location, version, snapshot info, totals).
2. Last 5 snapshots (most recent first) with operation + key summary fields.
3. File-size histogram from manifests; default demo bins; `--size-bins` override with KB/MB/GB units.
4. Tests for default + custom bins.

**Done**
- Sections render as spec’d; histogram counts correct for seeded data.

---

### Milestone 7 — docker-compose Test Rig
**Tasks**
1. Compose services: MinIO, Lakekeeper, Seeder.
2. Seeder: create warehouse, namespaces/tables, upload tiny Parquet files, commit.
3. `TESTING.md` + Makefile targets: `infra-up`, `infra-seed`, `infra-logs`, `infra-down`, `infra-reset`.
4. Integration tests wired to use compose.

**Done**
- `make infra-reset` yields a working env; all CLI commands succeed.

---

### Milestone 8 — Distribution Wiring (no publish)
**Tasks**
1. Finalize `pyproject.toml` metadata (name, summary, license, classifiers, URLs).
2. Verify `console_scripts` exposes `icectl`.
3. `make build` → wheel + sdist in `dist/`.
4. Local install paths documented:
   - `make install` (uv tool install from local project).
   - Alternative: pipx from local project/wheel.
5. README “Install” section shows pipx and uv tool flows (no PyPI usage).

**Done**
- `icectl` appears on `$PATH` after `make install` (no PyPI).
- Uninstall instructions work (`make uninstall`).

---

### Milestone 9 — Polish & Docs (pre-TUI)
**Tasks**
1. Help texts audited; examples added.
2. Error messages standardized by domain (config vs catalog vs sampling).
3. README expanded: Make targets, config samples (incl. overrides), troubleshooting.
4. TUI planning notes (what core APIs the TUI will reuse).

**Done**
- New users can `make infra-reset` → run commands in minutes; docs are sufficient.

---

## 11) Risks & Mitigations

- **Compose drift:** pin image versions; idempotent seeder.
- **Arrow FS auth quirks:** document env vars for MinIO/AWS; provide troubleshooting.
- **Large manifests:** seed stays small; graceful timeouts.

---

## 12) After MVP (TUI Planning)

The TUI will call the **same `icelib` APIs**; only View-layer code differs. We’ll revisit UI/UX once Milestones 0–3 are merged and stable.

---

**End of design doc.**
