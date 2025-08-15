# ---------------------------
# RPS Analytics Sandbox — Friendly Makefile
# ---------------------------

SHELL := /bin/bash
.ONESHELL:
.SILENT: help
.DEFAULT_GOAL := help

# ------- Project knobs -------
SCALE   ?= small                 # small|medium
PROJECT ?= rps-analytics-sandbox
DC := docker compose -p $(PROJECT)

# Load env (optional) so Make sees the same vars docker-compose uses.
ifneq (,$(wildcard .env))
include .env
export
endif
ifneq (,$(wildcard .env.metabase))
include .env.metabase
export
endif

# Derived (fallbacks) for Metabase setup script.
ADMIN_EMAIL ?= $(MB_EMAIL)
ADMIN_PASS  ?= $(MB_PW)
RPS_HOST    ?= $(POSTGRES_HOST)
RPS_DB      ?= $(POSTGRES_DB)
RPS_USER    ?= $(POSTGRES_USER)
RPS_PASS    ?= $(POSTGRES_PASSWORD)

# ------- Phony targets -------
.PHONY: help docs
.PHONY: start up quickstart reset-hard bootstrap stop down clean nuke urls logs ps doctor
.PHONY: reseed dbt-build dbt-run dbt-clean app app-url
.PHONY: metabase-up metabase-down metabase-reset metabase-initdb metabase-url metabase-bootstrap metabase-wipe-db
.PHONY: psql db-shell
.PHONY: setup-dev fmt lint fix-sql check
# ================= HELP =================
help: ## Show this help (most used: start, quickstart, dbt-run, reseed, metabase-bootstrap)
	@printf "\n\033[1mRPS Analytics Sandbox — Make targets\033[0m\n\n"
	@awk 'BEGIN {FS":.*##"; OFS="";} /^[a-zA-Z0-9_.-]+:.*##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@printf "\n\033[1mCommon flows\033[0m\n"
	@printf "  \033[36mmake start\033[0m       → Postgres → init → generate → dbt build → Streamlit → Metabase\n"
	@printf "  \033[36mmake quickstart\033[0m  → Full wipe, then same as start (use when you want a clean slate)\n"
	@printf "  \033[36mmake reseed\033[0m      → Re-run generator (fresh synthetic data) and dbt build\n"
	@printf "  \033[36mmake dbt-run\033[0m     → Only (re)build dbt models (deps+build)\n"
	@printf "  \033[36mmake metabase-bootstrap\033[0m → Start MB + create MB DB (if needed) + API bootstrap\n"

docs: ## Print quick usage tips
	@printf "%s\n" \
	"Scenarios:" \
	"• First run / demo:       make quickstart" \
	"• New data only:          make reseed" \
	"• Only rebuild dbt:       make dbt-run" \
	"• After editing models:   make dbt-run" \
	"• Debug SQL quickly:      make psql    # or make db-shell" \
	"• Reset Metabase fully:   make metabase-reset (or metabase-wipe-db) then make metabase-bootstrap" \
	"• Full house clean:       make reset-hard" \
	"" \
	"Env knobs:" \
	"SCALE=medium make start          # bigger dataset" \
	"PROJECT=myproj make start        # change compose project name"

# ============== LIFECYCLE (end-to-end) ==============
start: ## Start stack without wiping data; also prints URLs
	@echo "⏫ Starting stack (Postgres → init → generate → dbt → Streamlit → Metabase)…"
	$(MAKE) bootstrap SCALE=$(SCALE)
	$(MAKE) app
	$(MAKE) metabase-up
	$(MAKE) urls

up: start ## Alias for start

quickstart: ## Full wipe then start (use for a clean slate)
	$(MAKE) reset-hard
	$(MAKE) start SCALE=$(SCALE)

reset-hard: ## Stop & prune everything (containers + volumes); then bootstrap
	$(DC) down -v
	$(MAKE) bootstrap SCALE=$(SCALE)

bootstrap: ## Init schema → seed synthetic data → build dbt (no wipe)
	$(DC) up -d postgres
	$(DC) run --rm db-init
	SCALE=$(SCALE) $(DC) run --rm generator
	$(DC) run --rm dbt deps
	$(DC) run --rm dbt build

stop: ## Stop app services (Streamlit & Metabase, keep Postgres running)
	-$(DC) stop streamlit metabase
	@echo "ℹ️  Postgres still running. Use 'make down' to stop all or 'make clean' to remove volumes."

down: ## Stop all containers (keep volumes)
	$(DC) down

clean: ## Stop all containers and remove volumes
	$(DC) down -v

nuke: clean ## Alias for clean

urls: ## Print service URLs
	$(MAKE) app-url
	$(MAKE) metabase-url

logs: ## Tail logs for all containers
	$(DC) logs -f --tail=200

ps: ## List containers
	$(DC) ps

doctor: ## Quick sanity: show env & ping containers
	@echo "Env:"
	@echo "  SCALE=$(SCALE)  PROJECT=$(PROJECT)"
	@echo "  DB=$(POSTGRES_DB)  HOST=$(POSTGRES_HOST)  PORT=$(POSTGRES_PORT)"
	@echo "  MB_EMAIL=$(ADMIN_EMAIL)"
	$(DC) ps
	@echo "Try: curl -sf http://localhost:3000/api/health || true"

# ================== DATA / DBT / APP ==================
reseed: ## Re-generate synthetic data & rebuild dbt (fast inner loop)
	SCALE=$(SCALE) $(DC) run --rm generator
	$(MAKE) dbt-run

dbt-build: dbt-run ## Back-compat alias

dbt-run: ## Rebuild dbt models (deps + build)
	$(DC) run --rm dbt deps
	$(DC) run --rm dbt build

dbt-clean: ## Remove dbt target & logs
	rm -rf dbt/target dbt/logs

app: ## Start Streamlit app
	$(DC) up -d streamlit

app-url: ## Show Streamlit URL
	@echo "📊 Streamlit → http://localhost:8501/"

# ================== METABASE ==================
metabase-up: ## Start Metabase container
	$(DC) up -d metabase
	$(MAKE) metabase-url

metabase-down: ## Stop Metabase
	-$(DC) stop metabase

metabase-reset: ## Delete ./metabase-data folder & restart Metabase (file-based state reset)
	-$(DC) stop metabase || true
	rm -rf metabase-data
	$(DC) up -d metabase

metabase-wipe-db: ## Drop & recreate Metabase DB in Postgres (DESTRUCTIVE)
	-$(DC) stop metabase || true
	$(DC) exec postgres bash -lc "psql -U $${POSTGRES_USER:-rps_user} -d postgres -c \"DROP DATABASE IF EXISTS metabase WITH (FORCE);\""
	$(DC) exec postgres bash -lc "psql -U $${POSTGRES_USER:-rps_user} -d postgres -c 'CREATE DATABASE metabase;'"

metabase-initdb: ## Ensure Metabase DB exists in Postgres
	$(DC) exec postgres bash -lc "psql -U $${POSTGRES_USER:-rps_user} -d postgres -tc \"SELECT 1 FROM pg_database WHERE datname='metabase'\" | grep -q 1 || psql -U $${POSTGRES_USER:-rps_user} -d postgres -c 'CREATE DATABASE metabase;'"

metabase-url: ## Show Metabase URL
	@echo "🔗 Metabase  → http://localhost:3000"

metabase-bootstrap: metabase-up metabase-url metabase-initdb ## One-shot API bootstrap (idempotent)
	MB_EMAIL="$(ADMIN_EMAIL)" MB_PW="$(ADMIN_PASS)" \
	POSTGRES_HOST="$(RPS_HOST)" POSTGRES_DB="$(RPS_DB)" POSTGRES_USER="$(RPS_USER)" POSTGRES_PASSWORD="$(RPS_PASS)" \
	python3 scripts/metabase_setup.py

# ================== UTILITIES ==================
psql: ## psql shell to warehouse DB
	$(DC) exec -e PGPASSWORD=$${POSTGRES_PASSWORD:-rps_password} postgres \
	psql -U $${POSTGRES_USER:-rps_user} -d $${POSTGRES_DB:-rps}

db-shell: ## Bash into Postgres container
	$(DC) exec postgres bash

# ====== DEV TOOLING ======
setup-dev: ## Install pre-commit, ruff, sqlfluff; install git hooks
	@echo "🛠  Installing dev tools with pipx (preferred) or pip..."
	@command -v pipx >/dev/null 2>&1 && pipx install pre-commit || pip install pre-commit
	@command -v pipx >/dev/null 2>&1 && pipx install ruff || pip install ruff
	@command -v pipx >/dev/null 2>&1 && pipx install sqlfluff || pip install sqlfluff
	pre-commit install
	@echo "💡 If you want Prettier hooks, ensure Node 18+ is installed."

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
