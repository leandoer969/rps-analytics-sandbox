# ---------------------------
# RPS Analytics Sandbox
# Optimized Makefile
# ---------------------------

SHELL := /bin/bash
.DEFAULT_GOAL := help

SCALE ?= small
PROJECT ?= rps-analytics-sandbox
DC := docker compose -p $(PROJECT)

.PHONY: \
	help \
	# lifecycle
	start up restart bootstrap stop down clean nuke urls logs \
	# pipeline
	dbt-build app app-url \
	# metabase
	metabase-up metabase-down metabase-reset metabase-initdb metabase-url \
	# dev tooling
	setup-dev fmt lint fix-sql check

# ========== HELP ==========
help: ## Show this help
	@printf "\n\033[1mRPS Analytics Sandbox — Make targets\033[0m\n\n"
	@awk 'BEGIN {FS":.*##"; OFS="";} /^[a-zA-Z0-9_.-]+:.*##/ { printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@printf "\n\033[1mCommon flows\033[0m\n"
	@printf "  \033[36mmake start\033[0m  → spin up stack (no wipe): Postgres → seed → dbt → Streamlit → Metabase\n"
	@printf "  \033[36mmake restart\033[0m→ full reset (wipe volumes) then rebuild & start Streamlit\n"
	@printf "  \033[36mmake stop\033[0m   → stop Streamlit & Metabase (keep Postgres running)\n"
	@printf "  \033[36mmake down\033[0m   → Stop all containers (keep volumes)\n"
	@printf "  \033[36mmake clean\033[0m  → stop all containers and remove volumes\n\n"

# ====== LIFECYCLE (end-to-end) ======
start: ## Start full stack without wiping data; prints URLs
	@echo "⏫ Starting full stack (Postgres → seed → dbt → Streamlit → Metabase)…"
	$(MAKE) bootstrap SCALE=$(SCALE)
	$(MAKE) app
	$(MAKE) metabase-up
	$(MAKE) urls

up: start ## Alias for start

restart: ## Full reset: drop volumes → init schema → seed → dbt build → start Streamlit
	$(DC) down -v
	$(DC) up -d postgres
	$(DC) run --rm db-init
	SCALE=$(SCALE) $(DC) run --rm generator
	$(DC) run --rm dbt deps
	$(DC) run --rm dbt build
	$(DC) up -d streamlit
	$(MAKE) app-url

bootstrap: ## Start/refresh without wiping volumes; init schema → seed → dbt build
	$(DC) up -d postgres
	$(DC) run --rm db-init
	SCALE=$(SCALE) $(DC) run --rm generator
	$(DC) run --rm dbt deps
	$(DC) run --rm dbt build

stop: ## Stop Streamlit & Metabase (Postgres keeps running)
	@echo "⏹  Stopping app services (Streamlit & Metabase)…"
	-$(DC) stop streamlit metabase
	@echo "ℹ️  Postgres is still running. Use 'make down' to stop all, or 'make clean' to remove volumes."

down: ## Stop all containers (keep volumes)
	$(DC) down

clean: ## Stop all containers and remove volumes (nuke)
	$(DC) down -v

nuke: clean ## Alias for clean

urls: ## Print service URLs
	$(MAKE) app-url
	$(MAKE) metabase-url

logs: ## Tail docker-compose logs
	$(DC) logs -f --tail=200

# ====== PIPELINE (dbt & app) ======
dbt-build: ## Rebuild dbt models (deps + build)
	$(DC) run --rm dbt deps
	$(DC) run --rm dbt build

app: ## Bring up Streamlit (requires Postgres running)
	$(DC) up -d streamlit

app-url: ## Echo Streamlit URL
	@echo "📊 Streamlit Dashboard → http://localhost:8501/"

# ====== METABASE ======
metabase-up: ## Start Metabase and print URL
	$(DC) up -d metabase
	$(MAKE) metabase-url

metabase-down: ## Stop Metabase
	-$(DC) stop metabase

metabase-reset: ## Reset file-based Metabase data (Option A); CAUTION: deletes ./metabase-data
	-$(DC) stop metabase || true
	rm -rf metabase-data
	$(DC) up -d metabase

metabase-initdb: ## Create Metabase DB (Option B) in Postgres if not exists
	$(DC) exec postgres bash -lc "psql -U rps_user -d postgres -tc \"SELECT 1 FROM pg_database WHERE datname='metabase'\" | grep -q 1 || psql -U rps_user -d postgres -c 'CREATE DATABASE metabase;'"

metabase-url: ## Echo Metabase URL
	@echo "🔗 Metabase → http://localhost:3000"

# ====== DEV TOOLING ======
setup-dev: ## Install pre-commit, ruff, sqlfluff; install git hooks
	@echo "Installing dev tools with pipx (recommended) or pip..."
	@command -v pipx >/dev/null 2>&1 && pipx install pre-commit || pip install pre-commit
	@command -v pipx >/dev/null 2>&1 && pipx install ruff || pip install ruff
	@command -v pipx >/dev/null 2>&1 && pipx install sqlfluff || pip install sqlfluff
	pre-commit install
	@echo "If you want Prettier hooks, ensure Node 18+ is installed."

fmt: ## Format & autofix Python (ruff)
	ruff format .
	ruff check . --fix

lint: ## Lint Python, SQL, YAML
	ruff check .
	sqlfluff lint dbt/models
	yamllint .

fix-sql: ## Autofix SQL formatting (dbt models)
	sqlfluff fix dbt/models

check: ## Run all pre-commit hooks on entire repo
	pre-commit run --all-files
