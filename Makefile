PY = uv run

.PHONY: deps hook fmt lint test itest check check-all build install uninstall dist-clean run icectl infra-up infra-down infra-logs infra-seed infra-reset itest-run

deps:
	uv sync

hook:
	$(PY) pre-commit install || true

fmt:
	$(PY) ruff format

lint:
	$(PY) ruff check --fix

test:
	$(PY) pytest -q

itest:
	$(PY) pytest -q -m integration

check: fmt lint test

check-all: fmt lint test itest

build:
	uv build

install:
	uv tool install --force .

uninstall:
	uv tool uninstall icectl || true

dist-clean:
	rm -rf dist/

# Dev execution
run icectl:
	$(PY) icectl $(ARGS)

# Docker Compose infra (MinIO + Lakekeeper + Seeder)
COMPOSE = docker-compose -f infra/docker-compose.yaml

infra-up:
	$(COMPOSE) up -d --build minio db lakekeeper-migrate lakekeeper minio-init

infra-seed:
	$(COMPOSE) run --rm seeder

infra-logs:
	$(COMPOSE) logs -f --tail=200

infra-down:
	$(COMPOSE) down -v

infra-reset: infra-down infra-up infra-seed

itest-run:
	bash scripts/run_integration.sh
