# Testing & Integration Guide

This project uses unit tests for fast feedback and a Docker-based integration environment (MinIO + Lakekeeper) for end-to-end validation.

## Unit Tests

Unit tests (default selection excludes integration tests by marker):
```sh
make test
```

Run with formatting/linting gates:
```sh
make check
```

- Tests are organized under `tests/`.
- Target Python is **3.12**.
- Linting/formatting uses **ruff**; pre-commit hooks will block commits on failure.

## Integration Tests & Environment (Docker)

Services:
- **MinIO** (S3-compatible object store)
- **Lakekeeper** (Apache Iceberg REST catalog)

The integration environment is managed via `docker compose` behind Make targets. A **Seeder** job populates small Parquet datasets and commits so metadata (e.g., `total-records`) and file size histograms are meaningful.

### Bring Up, Seed, and Verify

```sh
export MINIO_ROOT_USER=minioadmin
export MINIO_ROOT_PASSWORD=minioadmin
make infra-reset      # down + up + seed (idempotent)
make infra-logs       # optional: follow logs
```

Once seeded, verify basic CLI flows:

```sh
# Point CLI to the included sample config
export ICECTL_CONFIG=$(pwd)/infra/icectl.config.yaml

icectl catalogs list
icectl catalogs show --catalog prod
icectl db list --catalog prod
icectl tables list --catalog prod --db analytics
icectl table schema analytics.events --catalog prod
icectl table sample analytics.events --catalog prod -n 5
icectl table describe analytics.events --catalog prod
```

Run only integration tests (separate step):

```sh
make itest        # runs tests marked with `@pytest.mark.integration`
```

### Configuration for Integration

Create `~/.config/icectl/config.yaml` (or set `ICECTL_CONFIG`) similar to:

```yaml
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

    overrides:
      warehouse: s3://warehouse/
      s3.endpoint: http://localhost:9000
      s3.path-style-access: true
      s3.region: us-east-1
      s3.access-key-id: ${MINIO_ROOT_USER}
      s3.secret-access-key: ${MINIO_ROOT_PASSWORD}
```

Alternatively, use the provided sample at `infra/icectl.config.yaml` and set `ICECTL_CONFIG` to that path.

> Secrets should be provided via env vars (e.g., export `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD` before running).

### Health & Logs

- `make infra-logs` to tail logs for all services.
- `make infra-down` to shut down and clean volumes.
- If Warehouse creation fails, consult Lakekeeper docs:
  - Creating a warehouse: <https://docs.lakekeeper.io/getting-started/#creating-a-warehouse>
  - Compose reference: <https://github.com/lakekeeper/lakekeeper/blob/main/docker-compose/docker-compose.yaml>

### Test Expectations

- Metadata-only commands (`tables list`, `table describe` sections) should not read data files.
- `table sample` reads minimal rows via PyArrow (no pandas) and should respect `--limit` and `--columns` (dot paths).
- Snapshot IDs are full-length; timestamps are ISO-8601 `Z`.

### Common Issues

- **Credentials**: Ensure env vars are set or AWS profiles are present if using AWS; for MinIO, use root user/password envs.
- **Networking**: If running compose on a remote Docker host, adjust `uri` and `endpoint_url` to reachable addresses.
- **Permissions**: Ensure MinIO buckets exist and are accessible; the Seeder should create/populate as needed.

---

## CI (Optional)

If CI is added, recommended stages:
1. `make check` (format, lint, unit tests)
2. Integration job (opt-in): `make infra-up`, `make infra-seed`, run a subset of CLI flows, `make infra-down`

Keep images pinned in compose to avoid drift; Seeder must be idempotent.
