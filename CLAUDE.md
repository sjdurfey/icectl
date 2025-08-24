# Repository Guidelines

This repository hosts Iceberg Navigator (`icectl`), a Python CLI for exploring Apache Iceberg catalogs. The CLI is documented in README and designed per DESIGN.md. The current tree is light while the CLI is implemented; use this guide to contribute safely and consistently.

## Project Structure & Module Organization

- Root: documentation (`README.md`, `DESIGN.md`, `TESTING.md`), project config (`pyproject.toml`).
- Code: temporary stub `hello.py`; the CLI and library will live under `src/` when wired.
- Tests: `tests/` (pytest; add alongside modules you touch).
- Infra (planned): Docker Compose files for MinIO/Lakekeeper used in integration testing.

## Build, Test, and Development Commands

Use uv for all workflows (Python 3.12 recommended):

```sh
uv sync                 # create/sync venv
uv run ruff format      # format
uv run ruff check --fix # lint and autofix
uv run pytest -q        # unit tests
uv build                # build wheel/sdist → dist/
uv tool install --force --from . icectl  # install CLI locally
```

Tip: until a Makefile lands, use the raw uv commands above. See README for intended Make targets and integration flows.

## Coding Style & Naming Conventions

- Style: ruff for lint + format (PEP8-ish, 4-space indent, 88–120 col where reasonable).
- Python: type hints required on public functions; prefer dataclasses for simple carriers.
- Naming: `snake_case` for functions/vars, `PascalCase` for classes, `SCREAMING_SNAKE_CASE` for constants.
- Layout: library code under `src/icelib/`, CLI under `src/icectl/` (entry points use Click).

## Testing Guidelines

- Framework: pytest. Place tests in `tests/` mirroring package paths (`tests/icelib/test_catalogs.py`).
- Names: files `test_*.py`; tests `test_*` functions.
- Coverage: no hard gate yet; include happy-path and edge cases; avoid network in unit tests.
- Integration: see TESTING.md for Docker-based MinIO + Lakekeeper; keep seeding idempotent.

## Commit & Pull Request Guidelines

- Commits: use Conventional Commits (e.g., `feat(cli): add tables list`, `fix(core): handle empty schema`).
- PRs: include a clear description, linked issues (`Closes #123`), and sample CLI output or screenshots when UX changes.
- Checks: run `uv run ruff format && uv run ruff check --fix && uv run pytest -q` before requesting review.
- Scope: small, focused PRs; update docs when behavior or flags change.

## Security & Configuration Tips

- Do not commit secrets; reference env vars in YAML (see README/TESTING examples).
- Prefer local endpoints (`localhost`) for integration. Redact secret-like keys in output paths and examples.

