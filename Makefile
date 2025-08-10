.PHONY: setup-dev fmt lint fix-sql check

setup-dev:
	@echo "Installing dev tools with pipx (recommended) or pip..."
	@command -v pipx >/dev/null 2>&1 && pipx install pre-commit || pip install pre-commit
	@command -v pipx >/dev/null 2>&1 && pipx install ruff || pip install ruff
	@command -v pipx >/dev/null 2>&1 && pipx install sqlfluff || pip install sqlfluff
	pre-commit install
	@echo "If you want Prettier hooks, ensure Node 18+ is installed."

fmt:
	ruff format .
	ruff check . --fix

lint:
	ruff check .
	sqlfluff lint dbt/models
	yamllint .

fix-sql:
	sqlfluff fix dbt/models

check:
	pre-commit run --all-files
