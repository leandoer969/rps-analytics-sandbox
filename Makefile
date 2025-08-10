SCALE ?= small
PROJECT ?= rps-analytics-sandbox

.PHONY: restart bootstrap dbt-build app down clean logs setup-dev fmt lint fix-sql check

# Full reset → init schema → seed → dbt build → start Streamlit
restart:
	docker compose down -v
	docker compose up -d postgres
	docker compose run --rm db-init
	SCALE=$(SCALE) docker compose run --rm generator
	docker compose run --rm dbt deps
	docker compose run --rm dbt build
	docker compose up -d streamlit

# Start from existing volumes, re-seed + rebuild
bootstrap:
	docker compose up -d postgres
	docker compose run --rm db-init
	SCALE=$(SCALE) docker compose run --rm generator
	docker compose run --rm dbt deps
	docker compose run --rm dbt build

# Rebuild dbt models only
dbt-build:
	docker compose run --rm dbt deps
	docker compose run --rm dbt build

# Bring up Streamlit (DB must already be up)
app:
	docker compose up -d streamlit

# Tear down containers (keep volumes)
down:
	docker compose down

# Tear down containers + volumes
clean:
	docker compose down -v

# Tail logs
logs:
	docker compose logs -f --tail=200

# Dev tooling bootstrap
setup-dev:
	@echo "Installing dev tools with pipx (recommended) or pip..."
	@command -v pipx >/dev/null 2>&1 && pipx install pre-commit || pip install pre-commit
	@command -v pipx >/dev/null 2>&1 && pipx install ruff || pip install ruff
	@command -v pipx >/dev/null 2>&1 && pipx install sqlfluff || pip install sqlfluff
	pre-commit install
	@echo "If you want Prettier hooks, ensure Node 18+ is installed."

# Format + autofix Python
fmt:
	ruff format .
	ruff check . --fix

# Lint everything (source SQL only)
lint:
	ruff check .
	sqlfluff lint dbt/models
	yamllint .

# Autofix SQL formatting (source models)
fix-sql:
	sqlfluff fix dbt/models

# Run all pre-commit hooks
check:
	pre-commit run --all-files
