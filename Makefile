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
	LAKEKEEPER_URI=http://localhost:8181/catalog/ \
	WAREHOUSE=s3://warehouse/ \
	MINIO_ROOT_USER=admin \
	MINIO_ROOT_PASSWORD=password \
	AWS_REGION=us-east-1 \
	PYTHONPATH=src \
	$(PY) python infra/seeder.py

infra-logs:
	$(COMPOSE) logs -f --tail=200

infra-down:
	$(COMPOSE) down -v

infra-reset: infra-down infra-up infra-seed

infra-test:
	@echo "=== Complete End-to-End Test with Static Project ID ==="
	@echo ""
	$(MAKE) infra-reset
	@echo ""
	@echo "Testing CLI commands..."
	@ICECTL_CONFIG=infra/icectl.config.yaml PYTHONPATH=src $(PY) python -c "\
	import sys; \
	sys.path.insert(0, 'src'); \
	from icectl.cli import cli; \
	print('✅ Catalogs:'); cli(['catalogs', 'list']); print(''); \
	print('✅ Namespaces:'); cli(['db', 'list']); print(''); \
	print('✅ Tables:'); cli(['tables', 'list', '--db', 'analytics']); print(''); \
	print('✅ Schema:'); cli(['tables', 'schema', 'analytics.events']); print(''); \
	print('✅ Description:'); cli(['tables', 'describe', 'analytics.events']); print(''); \
	"
	@echo "=== ✅ All tests passed! Static project ID: 00000000-0000-0000-0000-000000000000 ==="

itest-run:
	bash scripts/run_integration.sh
